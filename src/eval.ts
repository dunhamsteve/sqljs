import { parser } from "./parser.js";
import { Database } from "./sqlite.js";
import { Expr, Schema, O, QName, Value, Row, Tuple, Index, Infix } from "./types.js";
import { assert, jlog } from "./util.js";

type TableInfo = {
    state: number
    schema: Schema
    need: Set<string> // read or projected
    left: boolean
    as: string
    constraint?: Infix
    index?: Index
}

function *initial(): Generator<Value[]> {
    yield []
}

function eachName(expr: O<Expr>, fn: (e:QName) => void) {
    if (!expr) return
    switch (expr[0]) {
        case 'IN':  eachName(expr[1], fn); break
        case 'PFX': eachName(expr[2], fn); break;
        case 'IFX': eachName(expr[2], fn); eachName(expr[3], fn); break
        case 'QN': fn(expr); break
    }
}

type Constraint = {
    expr: Expr
    done: boolean
    // additional bookkeeping, indexes, tables, etc
}

// v1, this just does everything
// v2, we'll return a plan and execute it (with args) below
export function *prepare(db: Database, sql: string) {
    let query = parser(sql)
    // jlog(query)

    let col2table: Record<string,string[]> = {}
    let name2table: Record<string,TableInfo> = {}
    let tables: TableInfo[] = []
    
    for (let {name,left,as} of query.from) {
        let schema = db.tables[name]
        assert(schema, `can't find table ${name}`)
        assert(as, `empty as for ${name}`)
        const info: TableInfo = { as, schema, left, need: new Set(), state: 0 };
        name2table[as] = info
        tables.push(info)

        for (let col of schema.columns) {
            console.log(name,as,col)
            col2table[col] ||= []
            col2table[col].push(as)
        }
    }
    
    // Markup the table names for unqualified references
    function qualify(expr?: Expr) {
        eachName(expr, (expr)=> {
            if (!expr[1]) {
                let n = expr[2]
                let names = col2table[n]
                assert(names, `unknown column ${n}`)
                assert(names.length == 1, `ambiguous name ${n} is in tables ${names}`)
                expr[1] = names[0]
            }
            name2table[expr[1]].need.add(expr[2])
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
    let constraints: Constraint[] = []
    function flat(expr?: Expr) {
        if (!expr) return
        if (expr[0] == 'IFX' && expr [1] == 'AND') {
            flat(expr[2])
            flat(expr[3])
        } else {
            constraints.push({
                expr,
                done: false
            })
        }
    }
    flat(query.where)
    // try to topo-sort the tables
    let plan: TableInfo[] = []
    function visit(info: TableInfo) {
        // Here a dependent is pointing back at us, the constraint can't be resolved until we get to our table
        if (info.state == 1) console.log('LOOP', info.as)
        if (info.state > 0) return
        let {indexes} = info.schema
        info.state = 1 // scanning
        for (let constraint of constraints) {
            let {expr} = constraint
            if (expr[0] == 'IFX') {
                if (expr[3][0] == 'QN' && expr[3][1] == info.as) {
                    // Flip expression so our QName comes first
                    let op = {"=":"=","<":">","<=":">=",">=":"<=",">":"<"}[expr[1]]
                    if (op) constraint.expr = expr = ['IFX',op,expr[3],expr[2]]
                }
                if (expr[2][0] == 'QN' && expr[2][1] == info.as) {
                    let op = expr[1]
                    let field = expr[2][2]
                    let right = expr[3]
                    let index = indexes.find(ix => ix.columns[0] == field)
                    // These are the operations we know at the moment
                    // We'll handle backwards and like at some point
                    if (index && ['=','<','<='].includes(op)) {
                        // we grab the first matching index and run with it
                        console.log('MATCH', info.as, field, 'for', expr)
                        constraint.done = true // pre-mark this
                        eachName(right, qname => qname[1] && visit(name2table[qname[1]]))
                        info.constraint = expr
                        info.index = index
                        break
                    } else if (index) { console.log(`Can't index for ${op} on ${info.as}.${field}`) }
                }
            }
        }
        plan.push(info)
        info.state = 2 // done
    }
    tables.forEach(visit)
    plan.reverse()

    console.log('order', plan.map(info => info.as))
    console.log('constraints', constraints)
    let fields: string[] = []
    function isFree(expr: Expr) {
        let free = true
        eachName(expr, ([_,t,n]) => free = free && fields.includes(t+'.'+n))
        console.log('isFree', expr, free)
        return free
    }

    function eval_(tuple: Tuple, expr: Expr): any {
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

    // project contains 0 for rowid or 1-based index for 
    function *scan(input: Generator<Tuple>, table: TableInfo, project: number[]) {
        for (let inTuple of input) {
            for (let [rowid, newTuple] of db.walk(table.schema.page)) {
                if (table.schema.idcol>=0) newTuple[table.schema.idcol] = rowid
                yield inTuple.concat(project.map(i => i ? newTuple[i-1] : rowid))
            }
        }
    }

  
    function *filter(output: Generator<Tuple>, constraint: Expr) {
        for (let tuple of output) {
            if (eval_(tuple, constraint)) yield tuple
        }
    }

    function *rowidEq(input: Generator<Tuple>, index: Index, constraint: Expr, project: number[]) {
        for (let inTuple of input) {
            let rowid = eval_(inTuple, constraint)
            let newTuple = db.lookupRow(index.page, rowid)
            yield inTuple.concat(project.map(i=> newTuple[i]))
            
        }
    }

    // we need to identify rowid constraints and reorder the tables
    // TODO - On clauses, etc
    // we should start with a dummy table (one empty tuple) and attach bare constraints
    
    // So we loop over the tables in order, building up an async generator
    // Try to pick good indexes, discharge constraints as soon as possible.
    let output = initial()
    for (;;) {
        // discharge available constraints
        // TODO - precalc this (above) and move into plan
        // Plan will be sequence of index, filter, whatever
        for (let constraint of constraints) {
            if (!constraint.done && isFree(constraint.expr)) {
                console.log('discharge', constraint)
                output = filter(output, constraint.expr)
                constraint.done = true
            }
        }
        const table = plan.pop()
        if (!table) break
        // find appropriate index or scan table
        console.log(table.as, 'CONSTRAINT', table.constraint)
        if (table.constraint && table.index) {
            let op = table.constraint[1]
            let ty = table.index.type
            if (ty == 'rowid') {
                if (op == '=') {
                    let project: number[] = []
                    table.index.columns.forEach((v,i) => {
                        if (table.need.has(v)) {
                            fields.push(table.as+'.'+v)
                            project.push(i)
                        }
                    })
                    output = rowidEq(output, table.index, table.constraint[3], project)
                } else {
                    assert(false, `unhandled rowid op ${op}`)
                }
                
            }
            // ******** run the constraint.


            // need to handle the rowid join here, or add it to the plan..
            // would be nice to discharge any filters before doing the join.
        } else {
            // move scan up here

        }
        // between index read and rowid join discharge any constraints you can

        console.log('SCAN',table)
        let project: number[] = []
        table.schema.columns.forEach((v,i) => {
            if (table.need.has(v)) {
                fields.push(table.as+'.'+v)
                project.push(i+1)
            }
        })
        output = scan(output, table, project)
    }
    // TODO - project
    // yield *output

    // and sort/distinct/etc
}
