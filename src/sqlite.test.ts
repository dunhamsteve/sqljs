// This reads sqlite files and can return full table contents.
// Search, index, and write support are left to the reader as an exercise.
// This is coded for nodejs Buffer and needs to be adjusted for ArrayBuffer/DataView
import {Database} from './sqlite.js'
import {readFileSync} from 'fs'
import { jlog } from './util.js';
//let {Database} = require('./sqlite');
// let fs = require('fs');
if (false) {
    let data = readFileSync('database.sqlite');
    let pager = new Database(data.buffer);
    console.log(pager.getTable('ZFSNOTE'));
    
} else { 
    let data
    // data = readFileSync('cases/test.db');
    data = readFileSync('cases/chinook.db');
    // data = readFileSync('cases/Photos.sqlite')
    let db = new Database(data.buffer);
    for (let key in db.tables) {
        console.log(`[${key}]`)
        console.log(db.getTable(key));
        console.log()
    }
    jlog(db.tables)
    
}





