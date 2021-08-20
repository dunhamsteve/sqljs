// This reads sqlite files and dump tables as json.
// Search, index, and write support are not implemented.
// This is coded for nodejs Buffer and needs to be adjusted for ArrayBuffer/DataView

import {jlog,assert, search} from './util.js'

let td = new TextDecoder()

type Row = Record<string,Value>
type Value = string | number | null | DataView
type Schema = {
    page: number
    columns: string[]
    pks: string[]
    idcol: number
    rowid: boolean
    indexes: Index[]
}
type Index = {
    page: number
    columns: string[]
}
interface Node {
    type: Number
    data: DataView
    start: number
    nCells: number
    cellStart: number
    right: number
}

// turns a raw row into an object (tagged with column names)
// pull this out into scanner when we do this for real
function zip(a: string[],b: Value[]) {
    let rval: Row = {};
    a.forEach((v,i)=>rval[v]=b[i]);
    return rval;
}

// slice for a DataView
function sliceView(view: DataView, start: number, end: number) {
    return new DataView(view.buffer, view.byteOffset+start, end-start);
}

// copy for a DataView
function copyView(dest: DataView, doff: number, source: DataView, soff: number, len: number) {
    for (let i = 0; i< len; i++) {
        dest.setUint8(doff+i, source.getUint8(soff+i));
    }
}

// Parses column names and pk/rowid info out of create table ddl
export function parse(page: number, sql: string): Schema {
    const toks = sql.toLowerCase().match(/\w+|"[^"]*"|\S/g) || []
    let s = 0
    let columns:string[] = []
    let name = ''
    let rowid = true
    let pks: string[] = []
    let prev =''
    let idcol = -1
    for (let t of toks) {
        let x = s+t
             if (x=='0(') s = 1
        else if (x=='1primary') s = 3
        else if (x=='3(') s=4
        else if (x=='4,') {}
        else if (x=='4)') s=2
        else if (x=='2(') s=5 // paren in column line
        else if (x=='5)') s=2
        else if (x=='2)') s=0
        else if (x=='2primary') { pks.push(name); if (prev=='integer') idcol=columns.length }
        else if (x=='1,'||x=='2,') s=1
        else if (x=='0without') rowid=false
        else if (s==1) { columns.push(name=t); s=2 }
        else if (s==4) pks.push(t)
        // else console.log(`skip "${x}"`)
        prev = t
    }
    // move pks to front for without rowid case (to match the table on disk)
    if (!rowid) { columns = pks.concat(columns.filter(x => !pks.includes(x))) }
    return {page,columns,pks,rowid,idcol,indexes:[]}
}

export class Database {
    data: DataView
    pageSize: number
    encoding: number
    reserved: number
    tables: Record<string,Schema>
    // indexes: Record<string,Index>
    constructor(buffer: ArrayBuffer) {
        let data = new DataView(buffer);
        this.data = data;

        let magic = td.decode(sliceView(data,0,16));
        assert('SQLite format 3\u0000' == magic, "Bad header magic");
        this.pageSize = data.getUint8(16)*256+data.getUint8(17)*65536;
        this.encoding = data.getUint32(56);
        assert(this.encoding == 1, 'only utf8 is supported');
        
        // This is extra space in each page for stuff like encryption
        this.reserved = data.getUint8(20); assert(this.reserved == 0, "reserved not implemented");
        this.tables = {};
        this.walk(1, (_,row) => {
            let [type,name,_table,page,sql] = row;
            if (type == 'table') { this.tables[name] = parse(page, sql) }
        });
        this.walk(1, (_,row) => {
            let [type,name,table,page,sql] = row;
            let t = this.tables[table]
            if (!t) {
                console.error(`no table ${table} for index ${name}`)
                return
            }
            if (type !== 'index') return
            let m = name.match(/sqlite_autoindex_(.*)_1/)
            if (sql) {
                t.indexes.push(parse(page,sql))
            } else if (m) {
                t.indexes.push({ page, columns: t.pks.concat(['rowid']) })
            } else {
                console.error('stray index with no sql', name)
            }
        });
    }
    getTable(name: string) {
        let table = this.tables[name];
        let rval: Row[] = [];
        this.walk(table.page, (key,row) => {
            // For integer primary key, it's null in the row and you use the rowid
            if (table.idcol >= 0 && row[table.idcol] == null) row[table.idcol] = key;
            rval.push(zip(table.columns,row));
        });
        return rval;
    }
    getPage(number: number) {
        return new DataView(this.data.buffer, (number-1)*this.pageSize, this.pageSize);
    }
    getNode(i: number) {
        let data = this.getPage(i)
        let start = i == 1 ? 100 : 0
        let type = data.getUint8(start);
        let nCells = data.getUint16(start+3);
        let cellStart = data.getUint16(start+5);
        let right = data.getUint32(start+8); // only valid for types 2,5
        return { type, data, start, nCells, cellStart, right }
    }
    // return a cursor at key (or node that would follow key) where key is a prefix of the cell tuples
    // do we want a separate one for rowid?
    // this is for searching an index or materialized view (without rowid tables)
    // generator will be our "cursor"
    *seek(i: number, needle: Value[]): Generator<Value[]>  {
        let node = this.getNode(i)
        let {type,nCells} = node
        assert(type == 2 || type == 10, 'scanning non index node')
        let ix = search(nCells, (i) => {
            let cell = this.cell(node,i)
            assert(cell.payload)
            let tuple = decode(cell.payload)
            for (let i=0;i<needle.length;i++) {
                if (needle[i] < tuple[i]) return true
                if (needle[i] > tuple[i]) return false
            }
            return true
        })
        // seek left if necessary
        if (type == 2 && ix < nCells) {
            let cell = this.cell(node,ix)
            if (cell.payload) {
                let tuple = decode(cell.payload)
                if (-1 != needle.findIndex((v,i) => v != tuple[i])) {
                    console.log('recurse left', needle, '<', tuple)
                    yield *this.seek(cell.left, needle)
                }
            }
        }
        if (ix < nCells) {
            // scan the rest
            for (;ix < nCells;ix++) {
                let cell = this.cell(node,ix)
                if (type == 2) { yield *this.scan(cell.left) }
                if (cell.payload) {
                    yield decode(cell.payload)  // the key is 0 / undefined for index cells
                }
            }
            if (type == 2) yield *this.scan(node.right)
        } else {
            yield *this.seek(node.right, needle)
        }
        
    }
    *scan(i: number): Generator<Value[]> {
        let node = this.getNode(i)
        let {type,nCells} = node
        assert(type == 2 || type == 10, 'scanning non index node')
        let ix = 0
        // scan the rest
        for (;ix < nCells;ix++) {
            let cell = this.cell(node,ix)
            if (type == 2) { yield *this.scan(cell.left) }
            if (cell.payload) {
                let tuple = decode(cell.payload)
                yield decode(cell.payload)  // the key is 0 / undefined for index cells
            }
        }
        if (type == 2) yield *this.scan(node.right)
    }
    // walk a btree rooted at block i
    walk(i: number, fn: (id:number,row:any[]) => void) {
        let node = this.getNode(i);
        let cells = this.cells(node)
        if (node.type == 5) {
            for (let cell of cells)
                this.walk(cell.left, fn);
            this.walk(node.right, fn);
        } else if (node.type == 13) {
            for (let cell of cells) 
                fn(cell.key, decode(cell.payload));
        } else if (node.type == 10) {
            for (let cell of cells) {
                fn(cell.key, decode(cell.payload))
            }
        } else if (node.type == 2) {
            for (let cell of cells) {
                this.walk(cell.left, fn)
                fn(cell.key, decode(cell.payload))
            }
            this.walk(node.right, fn)
        } else {
            throw Error(`unhandled node type ${node.type} in page ${i}`)
        }
    }
    // Get all the cells in a Node
    cells(node: Node) {
        let cells: any[] = [];
        for (let i=0;i<node.nCells;i++) {
            cells.push(this.cell(node, i));
        }
        return cells;   
    }

    cell(page: Node, i: number) {
        let ptr = page.start + ((page.type<8)?12:8); 
        let data = page.data;
        let pos = data.getUint16(ptr+2*i);
        let type = page.type
        
        function varint() { // supposed to be signed and js doesn't handle 64 bits...
            let rval = 0;
            for (let j=0;j<8;j++) {
                let v = data.getUint8(pos++);
                rval = (rval << 7) | (v&0x7f);
                if ((v & 0x80) == 0)
                    return rval;
            }
            return (rval << 8) | data.getUint8(pos++);
        }
        function u32() {
            pos += 4;
            return data.getUint32(pos-4);
        }
        let left = (type == 5 || type == 2) ? u32() : 0
        let tlen = type == 5 ? 0 : varint();
        let key =  (type == 13 || type == 5) ? varint() : 0;
        let payload
        if (type !== 5) {
            let u = data.byteLength; // REVIEW - this right if we're on the first page?
            let x = page.type === 13 ? u-35 : (((u-12)*64/255)|0)-23;
            if (tlen <= x) {
                payload = sliceView(data,pos,pos+tlen);
            } else {
                payload = new DataView(new ArrayBuffer(tlen));
                let rest = tlen;
                let offset = 0;
                let m = (((u-12)*32/255)|0)-23;
                let k = m + ((tlen-m)%(u-4));
                let l = (k>x) ? m : k;
                copyView(payload, offset, data, pos, l);
                offset += l;
                pos += l;
                rest -= l;
                for (let over = data.getUint32(pos);over != 0;over = data.getUint32(0)) {
                    data = this.getPage(over);
                    pos = 4;
                    if (rest < u - 4) {
                        copyView(payload, offset, data, 4, rest);
                        break;
                    } else {
                        copyView(payload, offset, data, 4, u-4);
                        rest -= u-4;
                        offset += u-4;
                    }
                }
            }
            assert(payload.byteLength == tlen, "Length mismatch");    
        }
        return {left,tlen,key,payload};
    }
}



export function decode(data: DataView): Value[] {
    let pos = 0;
    function varint() {
        let rval = 0;
        for (let j=0;j<8;j++) {
            let v = data.getUint8(pos++);
            rval = (rval << 7) | (v&0x7f);
            if ((v & 0x80) == 0)
                return rval;
        }
        return (rval << 8) | data.getUint8(pos++);
    }

    let hlen = varint();
    let hend = hlen;
    let types = [];
    while (pos < hend) types.push(varint());
    let row = types.map(t => {
        switch (t) {
            case 0:
                return null;
            // FIXME - 64 bits don't fit into javascript integers.
            case 1: case 2: case 3:
            case 4: case 5: case 6: 
                var mask = 0x80;
                var value = data.getUint8(pos++);
                for (;t>1;t--) {
                    mask = mask << 8;
                    value = (value<<8)+data.getUint8(pos++);
                }
                return (value&mask) ? (value-2*mask) : value;
            case 7:
                pos += 8;
                return data.getFloat64(pos-8); // BE?
            case 8:
                return 0;
            case 9:
                return 1;
            default:
                if (t > 11) {
                    let len = (t-12)>>1;
                    let v = sliceView(data,pos,pos+len);
                    assert(v.byteLength == len, 'length mismatch');
                    pos += len;
                    return t&1 ? td.decode(v) : v;
                }
                assert(false, "Bad type: "+t);
        }
    });
    return row;
}
