import { readFileSync, writeFileSync } from "fs";
import { execute } from "./eval.js";
import { Database } from "./sqlite.js";
import { jlog } from "./util.js";

// https://www.sqlitetutorial.net/sqlite-sample-database/

let data = readFileSync('cases/chinook.db')
let db = new Database(data.buffer)

let cur = db.seek(db.tables.artists.page,[])
for (let tuple of cur) {
    console.log(tuple.join('\t'))
}
