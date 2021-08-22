import { parser } from "./parser.js";
import { Database } from "./sqlite.js";
import { Expr, Schema, O, QName, Value, Row, Tuple } from "./types.js";
import { assert, jlog } from "./util.js";

type TableInfo = {
    schema: Schema
    need: Set<string> // read or projected
    left: boolean
    as: string
}

function *initial(): Generator<Value[]> {
    yield []
}

// v1, this just does everything
// v2, we'll return a plan and execute it (with args) below
export function *prepare(db: Database, sql: string) {
    let query = parser(sql)
    // jlog(query)

    let col2table: Record<string,string[]> = {}
    let tables: Record<string,TableInfo> = {}
    let stack: TableInfo[] = []
    
    for (let {name,left,as} of query.from) {
        let schema = db.tables[name]
        assert(schema, `can't find table ${name}`)
        assert(as, `empty as for ${name}`)
        const info: TableInfo = { as, schema, left, need: new Set() };
        tables[as] = info
        stack.push(info)

        for (let col of schema.columns) {
            console.log(name,as,col)
            col2table[col] ||= []
            col2table[col].push(as)
        }
    }
    function eachName(expr: Expr, fn: (e:QName) => void) {
        if (!expr) return
        switch (expr[0]) {
            case 'IN':  eachName(expr[1], fn); break
            case 'PFX': eachName(expr[2], fn); break;
            case 'IFX': eachName(expr[2], fn); eachName(expr[3], fn); break
            case 'QN': fn(expr); break
        }
    }
    function qualify(expr?: Expr) {
        eachName(expr, (expr)=> {
            if (!expr[1]) {
                let n = expr[2]
                let names = col2table[n]
                assert(names, `unknown column ${n}`)
                assert(names.length == 1, `ambiguous name ${n} is in tables ${names}`)
                expr[1] = names[0]
            }
            tables[expr[1]].need.add(expr[2])
        })
    }
    query.select.forEach(qualify)
    qualify(query.where)
    query.from.forEach(f => qualify(f.on))
    jlog(query)

    // We're going to ignore ON and LEFT JOIN for the moment
    // How to handle OR?
    // So we have a list of tables that we'll run through in order

    // So I think SQLite is trying to turn ors into in clauses or between clauses
    // and then just top level ands.

    // at the very least we need to pick the constraints for each table
    function flat(expr?: Expr): Expr[] {
        if (!expr) return []
        if (expr[0] == 'IFX' && expr [1] == 'AND') {
            return flat(expr[2]).concat(flat(expr[3]))
        }
        return [expr]
    }

    let constraints = flat(query.where)
    console.log('constraints', constraints)
    let fields: string[] = []
    function isFree(expr: Expr) {
        let free = true
        eachName(expr, ([_,t,n]) => free = free && fields.includes(t+'.'+n))
        console.log('isFree', expr, free)
        return free
    }

    function getConstraints(): Expr[] {
        let rval: Expr[] = []
        let tmp: Expr[] = []
        for (let c of constraints) {
            (isFree(c)?rval:tmp).push(c)
        }
        constraints = tmp
        console.log('getConstraints', fields, rval)
        return rval
    }

    function *scan(input: Generator<Tuple>, table: TableInfo, project: number[]) {
        for (let inTuple of input) {
            for (let [rowid, newTuple] of db.walk(table.schema.page)) {
                if (table.schema.idcol>=0) newTuple[table.schema.idcol] = rowid
                yield inTuple.concat(project.map(i => newTuple[i]))
            }
        }
    }

    function eval_(tuple: Tuple, expr: Expr): any {
        debugger
        switch (expr[0]) {
            case 'LIT': return expr[1][1]
            case 'QN':  return tuple[fields.indexOf(expr[1]+'.'+expr[2])]
            case 'IFX':
                switch (expr[1]) {
                    case '=': return eval_(tuple, expr[2]) == eval_(tuple, expr[3])
                    case '<': return eval_(tuple, expr[2]) < eval_(tuple, expr[3])
                    case '>': return eval_(tuple, expr[2]) > eval_(tuple, expr[3])
                    default: assert(false, `unhandled binop ${expr[1]}`)
                }
        }
    }

    function *filter(output: Generator<Tuple>, constraint: Expr) {
        for (let tuple of output) {
            if (eval_(tuple, constraint)) yield tuple
        }
    }

    // TODO - On clauses, etc
    // we should start with a dummy table (one empty tuple) and attach bare constraints
    stack.reverse()
    
    // So we loop over the tables in order, building up an async generator
    // Try to pick good indexes, discharge constraints as soon as possible.
    let output = initial()
    for (;;) {
        // discharge available constraints
        
        for (let constraint of getConstraints()) {
            console.log('discharge', constraint)
            output = filter(output, constraint)
        }
        let table = stack.pop()
        if (!table) break
        // find appropriate index or scan table
        for (let index of table.schema.indexes) {
            // TODO
        }
        console.log('SCAN',table)
        let project: number[] = []
        table.schema.columns.forEach((v,i) => {
            if (table.need.has(v)) {
                fields.push(table.as+'.'+v)
                project.push(i)
            }
        })
        output = scan(output, table, project)
    }
    // TODO - project
    yield *output
    
    // and sort/distinct/etc
}
