
export function assert(value:unknown, msg: string = 'assert'): asserts value {  if (!value) throw new Error("Assert failed: "+msg); }
export let jlog = (x:any) => console.log(JSON.stringify(x,null,'  '))


export function search(n: number, fn: (ix:number) => boolean) {
    let i = 0, j = n
    while (i < j) {
        let h = (i + j)>>1
        if (!fn(h)) {
            i = h + 1
        } else {
            j = h
        }
    }
    return i
}