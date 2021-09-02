import { readFileSync, writeFileSync } from "fs";
import { execute } from "./eval.js";
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

type Case = [string, number, string]
let cases: Case[] = [
    ['scan',   275, 'select artistid, name from artists'],
    ['scan2',  347, 'select albumid, title from albums'],
    ['join2',  347, 'select name, title from artists, albums where albums.artistid = artists.artistid'],
    ['join3', 3503, 'select artists.name, title, tracks.name from artists, albums, tracks where albums.artistid = artists.artistid and tracks.albumid = albums.albumid'],
    ['join4', 3503, `select artists.name, title, tracks.name, genres.name
                     from artists, albums, tracks, genres 
                     where albums.artistid = artists.artistid and tracks.albumid = albums.albumid and tracks.genreid = genres.genreid`],
    ['great',  175, 'select rowid, name from artists where rowid > 100'],
    ['less',    99, 'select rowid, name from artists where rowid < 100'],
]



for (let [key,cnt,query] of cases) {
    console.log(`\n*** ${key}`)
    let rows = ''
    let count = 0
    for (let tuple of execute(db, query)) {
        rows += tuple.join('\t')+'\n'
        count++
    }
    console.log(`--- ${key} ${count} rows, expect ${cnt}`)
    writeFileSync(`tmp/${key}.tsv`, rows)
}
