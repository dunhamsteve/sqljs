import { readFileSync } from "fs";
import { prepare } from "./eval.js";
import { Database } from "./sqlite.js";
import { jlog } from "./util.js";

// https://www.sqlitetutorial.net/sqlite-sample-database/

let data = readFileSync('cases/chinook.db')
let db = new Database(data.buffer)

for (let k in db.tables) {
    let t =  db.tables[k]
    console.log('table', k, "\t",t.columns.join(', '))
    for (let ix of t.indexes) {
        console.log('-', ix.columns.join(', '))
    }
    console.log()
}
console.log('\n\n***\n\n')

// We have an index both ways here.  One is rowid, and one is standard index.
let sql ='select name, title from artists, albums where albums.artistid = artists.artistid'
// NEXT/TODO this is broken, figure out why
sql ='select artists.name, title, tracks.name from artists, albums, tracks where albums.artistid = artists.artistid and tracks.albumid = albums.albumid'
// sql = 'select artists.artistid, name from artists,albums where albums.artistid = artists.artistid'
for (let tuple of prepare(db, sql)) {
    console.log('-',tuple)
}
