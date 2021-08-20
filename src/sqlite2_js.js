// This reads sqlite files and can return full table contents.
// Search, index, and write support are left to the reader as an exercise.
// This is coded for nodejs Buffer and needs to be adjusted for ArrayBuffer/DataView
import {Database} from './sqlite2.js'
import {readFileSync} from 'fs'
//let {Database} = require('./sqlite');
// let fs = require('fs');
if (false) {
    let data = readFileSync('database.sqlite');
    let pager = new Database(data.buffer);
    console.log(pager.getTable('ZFSNOTE'));
    
} else {
    let data = readFileSync('test.db');
    // data = readFileSync('Photos.sqlite')
    let pager = new Database(data.buffer);
    for (let key in pager.tables) {
        console.log(`[${key}]`)
        console.log(pager.getTable(key));
        console.log()
    }
    // pager.walk(1, (k,v) => {
    //     let sql=v[4]
    //     console.log(k,sql)
    //     if (sql) parse2(sql)
    // })
    
    
}





