
import {readFileSync} from 'fs'
import {Database} from './dist/sqlite.js'
import {jlog} from './dist/util.js'

let data = readFileSync('cases/database.sqlite')
let db = new Database(data.buffer)

db.walk(1, (k,v) => console.log(k,v[0],v[1],v[3],!!v[4]))

// db.walk(9, console.log)
// jlog(db.tables)
console.log('\n\n-----\n\n')
// for (let x of db)
for (let row of db.seek(9,[577,90])) {
    console.log(row)
}
console.log('lookup',db.seek(9,[577,99]).next())