// This reads sqlite files and can return full table contents.
// Search, index, and write support are left to the reader as an exercise.
// This is coded for nodejs Buffer and needs to be adjusted for ArrayBuffer/DataView

// TODO - fix Z_PK - looks like there is a dummy null value?

function assert(value, msg) {  if (!value) throw new Error("Assert failed: "+msg); }

function zip(a,b) {
    let rval = {};
    a.forEach((v,i)=>rval[v]=b[i]);
    return rval;
}

function view2str(view,start,end) {
    let res = "";
    for (let pos = start; pos < end; pos++) {
        res += String.fromCharCode(view.getUint8(pos))
    }
    return res;
}
function sliceView(view, start, end) {
    return new DataView(view.buffer, view.byteOffset+start, end-start);
}

function copyView(dest, doff, source, soff, len) {
    for (let i = 0; i< len; i++) {
        dest.setUint8(doff+i, source.getUint8(soff+i));
    }
}

export class Database {
    constructor(buffer) {
        let data = new DataView(buffer);
        this.data = data;
        let magic = view2str(data,0,16);
        assert('SQLite format 3\u0000' == magic, "Bad header magic");
        this.pageSize = data.getUint8(16)*256+data.getUint8(17)*65536;
        this.encoding = data.getUint32(56); // we presume utf8!
        this.reserved = data.getUint8(20); assert(this.reserved == 0, "reserved not implemented");
        this.tables = {};
        this.walk(1, (key,row) => {
            if (row[0] == 'table') {
                let [type,name,table,page,sql] = row;
                let columns = sql.match(/\((.*)\)/)[1].trim().split(",").map(x => x.trim().split(" ")[0])
                this.tables[name] = {page,columns};
            }
        });
    }
    getTable(name) {
        var table = this.tables[name];
        var rval = [];
        this.walk(table.page, (key,row) => rval.push(zip(table.columns,row)));
        return rval;
    }
    getPage(number) {
        return new DataView(this.data.buffer, (number-1)*this.pageSize, this.pageSize);
    }

    node(i) {
        return new Node(this,i);
    }

    walk(i, fn) {
        let node = this.node(i);
        if (node.type == 5) {
            node.cells().forEach(cell => this.walk(cell.page, fn));
            this.walk(node.rightPtr, fn);
        }
        if (node.type == 13)
            node.cells().forEach(cell => fn(cell.key, decode(cell.payload)));
    }
}

class Node {
    constructor(pager, i) {
        let data = pager.getPage(i), start = i == 1 ? 100 : 0;
        this.pager = pager;
        this.data = data;
        this.start = start;
        this.reserved = pager.reserved;
        this.type = data.getUint8(start);
        this.nCells = data.getUint16(start+3);
        this.cellStart = data.getUint16(start+5);
        if (this.type < 8)
            this.rightPtr = data.getUint32(start+8); // only valid for types 2,5
    }

    cell(i) {
        let ptr = this.start + ((this.type<8)?12:8); 
        let data = this.data;
        let pos = data.getUint16(ptr+2*i);
        
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

        if (this.type == 5) {
            let page = data.getUint32(pos);pos += 4;
            let key = varint();
            return {page,key};
        }
        if (this.type == 13) {
            let tlen = varint();
            let key = varint();
            let u = data.byteLength; // REVIEW - this right if we're on the first page?
            let x = u-35; // FIXME - index records have a different x
            let payload;
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
                    data = this.pager.getPage(over);
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
            return {i,tlen,key,payload};
        }
        throw new Error("unimplemented page type "+this.type);
    }

    cells() {
        let cells = [];
        for (let i=0;i<this.nCells;i++) {
            cells.push(this.cell(i));
        }
        return cells;   
    }
}

function fix(v,mask) { return (v&mask) ? (v-2*mask) : v; }

function decode(data) {
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
            case 1: case 2: case 3:
            case 4: case 5: case 6: // FIXME - 64 bits don't fit in javascript integers.
                var mask = 0x80;
                var value = data.getUint8(pos++);
                for (;t>1;t--) {
                    mask = mask << 8;
                    value = (value<<8)+data.getUint8(pos++);
                }
                return fix(value,mask);
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
                    assert(v.byteLength == len);
                    if (t&1) v = view2str(v,0,len); // XXX convert via utf8..
                    pos += len;
                    return v;
                }
                assert(false, "Bad type: "+t);
        }
    });
    return row;
}


