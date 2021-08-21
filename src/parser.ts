type O<A> = A|undefined

type TableSpec = {
    name: string
    left: boolean
    on: unknown
    as: string
}

type Lit
    = ['NUM', number]
    | ['STR', string]

type Expr
    = ['QN', O<string>, string]
    | ['LIT', Lit]
    | ['IFX', string, Expr, Expr]
    | ['PFX', string, Expr]
    | ['IN', Expr, Lit[] ]

type Select = {
    projection: Expr[] // these are actually expressions.. 
    from: TableSpec[]
    where: Expr
}

type Pred = (k:string) => boolean

let reserved = ["on", "select", "left"]

// operator precedence table
// maybe fix & | unless sql demands it as is
// https://www.sqlite.org/lang_expr.html#operators_and_parse_affecting_attributes
type Ops = string[] & {tag?: any}
let tag = (tag:any, ops:Ops) => (ops.tag=tag,ops)

let operators = [
    tag('I', ["or"]),
    tag('I', ["and"]),
    tag('P', ['not']),
    tag('I', ["=", "==", "<>"]),
    tag('I', ["<",">","<=","<="]),
    tag('I', ["&", "|", "<<", ">>"]),
    tag('I', ["+","-"]),
    tag('I', ["*","/","%"]),
    tag('P', ["+","-"]),
]

export function parser(sql: string) {
    let assert = <A>(value:A, msg?: string) => {  
        if (!value) 
            throw new Error(`${msg??"parse error"} at ${toks[p]}`); 
        return value 
    }
    // refine this
    let toks = sql.toLowerCase().match(/\w+|"[^"]*"|[\d.]+|'[^']*'|\S/g) || []
    toks = toks.map(t => "\"'".includes(t[0])?t:t.toLowerCase())
    console.log(toks)
    let p = 0
    let isident = (x: string) => x.match(/^\w+$/) && !reserved.includes(x)
    let next = () => toks[p++]
    let pred = (p: Pred, msg:string) => { let n = next(); assert(p(n),msg); return n }
    let ident = () => pred(isident, 'expected ident')
    let expect = (k:string) => assert(next()==k,`expected ${k}`)
    let maybe = (k: string) => toks[p] == k && next()
    let pQName = (): Expr => {
        let ns
        let name = ident()
        if (maybe('.')) {
            ns = name, name = ident()
        }
        return ['QN', ns, name]
    }

    // TODO - need to do parens, thinking about flattening vs associativity of left join.
    let pSpec = (left: boolean): TableSpec => {
        let name = ident()
        let as
        if (isident(toks[p]) || maybe('as')) {
            as = ident()
        }
        let on
        if (maybe('on')) {
            on = pExpr()
        }
        return {name, left, as, on}
    }
    let pFrom = (): TableSpec[] => {
        let rval = [pSpec(false)]
        for (;;) {
            if (maybe(',')) {
                rval.push(pSpec(false))
            } else if (maybe('left')) {
                maybe('outer')
                expect('join')
                rval.push(pSpec(true))
            } else if (maybe('(')) {
                rval = rval.concat(pFrom())
                expect(')')
            } else {
                break
            }
        }
        return rval
    }
    let pSelect = (): Select => {
        expect("select")
        let projection = [ pExpr() ]
        while (maybe(',')) { projection.push(pExpr()) }
        let from = pFrom()
        expect('where')
        let where = pExpr()
        return {projection, from, where}
    }
    let isnumber = (k:string) => k.match(/^[\d.]+$/)
    let pAExpr = (): Expr => {
        let t = toks[p]
        if (isnumber(t)) { return ['LIT', ['NUM', Number(next())]] }
        if (isident(t))  { return pQName() }
        if (toks[p][0]=="'") { next(); return ['LIT',['STR', t.slice(1,t.length-1)]]}
        assert(false,'expected literal or identifier')
    }

    let maybeOper = (prec: number, tag: string): O<[string,number]> => {
        let q = p
        let op = toks[q++]
        // not foo infix is represented as "not_foo" in the table and ast
        if (op == 'not' && tag == 'I') op = 'not_'+toks[q++]
        for (;prec<operators.length;prec++) {
            let x = operators[prec]
            if (x.tag == tag && x.includes(op)) {
                p = q
                console.log('ISOP',op,prec,tag)
                return [op,prec]
            }
        }
        console.log('NOTOP',op,prec,tag)
    }

    // This is a pratt parser for expressions
    // TODO - postfix
    let pExpr = (prec=0): Expr => {
        let left: Expr
        let pfx = maybeOper(0,'P')
        if (pfx) {
            left = ['PFX', , pExpr(p)]
        } else {
            left = pAExpr()
        }
        for (;;) {
            let op = maybeOper(prec, 'I')
            if (!op) break
            left = ['IFX', op[0], left, pExpr(op[1]+1)]
        }
        return left
    }

    return pSelect()
}
