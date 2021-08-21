
import {parser} from './parser.js'
import { jlog } from './util.js'

let eg = [
    "select foo.bar, boo.baz from foo where foo.bar = 1 AND foo.blah = 2 OR foo.baz > 1",
]
for (let sql of eg) {
    console.log(sql)
    jlog(parser(sql))
}
