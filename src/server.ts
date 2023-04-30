import { readFileSync } from "fs"
import { Server } from "net"
import * as tls from 'tls'
import { Database } from "./sqlite.js"
import { execute } from "./eval.js"
import { parser } from "./parser.js";

let data = readFileSync('cases/chinook.db')
let db = new Database(data.buffer)

// I could paste this onto my sqlite3 implementation. :)


class Packet {
    buf: ArrayBuffer
    view: DataView
    pos = 0
    constructor(type: string) {
        this.buf = new ArrayBuffer(4096)
        this.view = new DataView(this.buf)
        this.char(type)
        // leave room for length
        this.i32(0)
    }
    i16(v: number) {
        this.view.setInt16(this.pos, v)
        this.pos += 2
    }
    i32(v: number) {
        this.view.setInt32(this.pos, v)
        this.pos += 4
    }
    char(s: string) {
        this.view.setInt8(this.pos++, s.charCodeAt(0))
    }
    bytes(bytes: Buffer) {
        this.i32(bytes.byteLength)
        for (let i = 0;i<bytes.byteLength; i++) {
            this.view.setInt8(this.pos++, bytes.readInt8(i))
        }
    }
    string(s: string) {
        let bytes = Buffer.from(s,'utf8')
        console.log('string',s, bytes, bytes.byteLength)
        for (let i = 0;i<bytes.byteLength; i++) {
            this.view.setInt8(this.pos++, bytes.readInt8(i))
        }
        this.view.setInt8(this.pos++,0)
    }
    getBytes() {
        let view = new DataView(this.buf)
        view.setInt32(1,this.pos-1)
        return new Uint8Array(this.buf.slice(0,this.pos))
    }
}

let key = readFileSync('private.key')
let cert = readFileSync('public.crt')

let server = new Server((socket) => {
    console.log('connected')
    let buf = Buffer.alloc(0)
    socket.on('end', () => {
        console.log('disconnected')
    })
    // no type byte for first packet
    let packetType: undefined | number = 83
    let length: undefined | number
    let payload: undefined | Buffer
    let init = true

    function processPacket(ptype: number, packetData: Buffer) {
        let ty = String.fromCharCode(ptype)
        console.log(ty, packetData.toString('hex'))
        let pos = 0
        let view = new DataView(packetData.buffer.slice(packetData.byteOffset, packetData.byteOffset + packetData.byteLength))
        let readI32 = () => {
            let v = view.getUint32(pos, false)
            pos += 4
            return v
        }
        let readString = () => {
            let end = packetData.indexOf(0, pos)
            let s = packetData.subarray(pos, end).toString('utf-8')
            pos = end + 1
            return s
        }
        if (ty == 'S') {
            let version = readI32()
            console.log('start', version, 'buf', buf.toString('hex'))
            if (version == 80877103) {
                // TLS Upgrade requested
                socket.removeAllListeners('data')
                socket.write('S', (err) => console.error('err', err))
                // again first packet without a tag
                init = true
                let secureSocket = new tls.TLSSocket(socket, {
                    cert, key, isServer: true,
                    // enableTrace: true,
                })
                socket = secureSocket
                secureSocket.on('data', handleData)
                secureSocket.on('error', console.error)
            } else {
                console.log(packetData.subarray(pos).toString())
                console.log({ pos })
                let data: Record<string, string> = {}

                while (pos < packetData.length) {
                    let k = readString()
                    if (!k) break
                    data[k] = readString()
                }
                console.log('connect config', data)
                socket.write("R\x00\x00\x00\x08\x00\x00\x00\x00")
                // TODO - send a bunch of S packets
                // TODO - send K (two i32)
                socket.write("Z\x00\x00\x00\x051")
            }
        }
        if (ty == 'Q') {
            // Gotta write our decode / encode.
            let sql = readString()
            let query = parser(sql)
            console.log('query',sql, query)
            // want column names..
            let res = execute(db, sql)
            let started = false
            
            let ix = 0
            function get_desc(v: any): [string, number, number, number, number, number, number] {
                ix++
                if (typeof v === 'number') {
                    return ['col'+ix, 0, 0, 23, 4, -1, 0]
                } else if (typeof v === 'string') {
                    return ['col'+ix, 0, 0, 24, -1, -1, 0]
                } else {
                    throw new Error(`FIXME ${typeof v} col spec`)
                }
            }
            let count = 0
            for (let tuple of res) {
                count++
                // console.log('row', tuple)
                if (!started) { 
                    let scheme = tuple.map(get_desc)
                    console.log('scheme', scheme)
                    let out = new Packet('T')
                    out.i16(scheme.length)
                    for (let [name,tab,col,typ,sz,mod,flag] of scheme) {
                        out.string(name)
                        out.i32(tab)
                        out.i16(col)
                        out.i32(typ)
                        out.i16(sz)
                        out.i32(mod)
                        out.i16(flag)
                    }
                    let x = out.getBytes()
                    console.log('send', x.toString(),x.length)
                    socket.write(x)
                    started = true
                }
                let out = new Packet('D')
                out.i16(tuple.length)
                for (let v of tuple) {
                    console.log(v,v.length)
                    if (typeof v === 'number') {
                        out.bytes(Buffer.from(v+'','utf8'))
                    } else if (typeof v === 'string') {
                        out.bytes(Buffer.from(v,'utf8'))
                    } else {
                        throw new Error(`FIXME ${typeof v} col spec`)
                    }
                }
                let x = out.getBytes()
                console.log('send', x.toString(),x.length, typeof x)
                socket.write(x)

                // desc T [sihi]
            }
            let out = new Packet('C')
            out.string(`SELECT ${count}`)
            let x = out.getBytes()
            console.log('send', x.toString(),x.length, typeof x)
            socket.write(x)

            out = new Packet('Z')
            
            out.char('1')
            x = out.getBytes()
            console.log('send', x.toString(),x.length, typeof x)
            socket.write(x)
        }
    }

    function handleData(data: Buffer) {
        console.log('handleData', data.toString('hex'))
        buf = Buffer.concat([buf, data])
        if (init) {
            packetType = 83
            init = false
        }
        if (packetType == undefined && buf.length > 0) {
            packetType = buf.readUInt8(0);
            buf = buf.subarray(1)
            // console.log({packetType})
        }
        if (length == undefined && buf.length > 4) {

            length = buf.readUInt32BE(0) - 4
            buf = buf.subarray(4)
            // console.log({length})
        }
        if (packetType != undefined && length !== undefined && payload == undefined && buf.length >= length) {
            let packetData = buf.subarray(0, length)
            processPacket(packetType, packetData);
            buf = buf.subarray(length)
            packetType = undefined
            length = undefined
        }
    }
    socket.on('data', handleData);
})
server.on('error', (err) => {
    console.error(err)
    throw err;
})
server.listen(4096, () => console.log('listening'))
