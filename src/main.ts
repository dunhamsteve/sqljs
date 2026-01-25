import { signal } from "@preact/signals";
import { Diagnostic } from "@codemirror/lint";
import { useEffect, useRef, useState } from "preact/hooks";
import { h, render, VNode } from "preact";
import { ChangeEvent } from "preact/compat";
import { execute } from './eval';
import { parser } from './parser';
import helpText from "./help.md?raw";
import { AbstractEditor, CMEditor, EditorDelegate } from "./cmeditor";
import { Expr } from "./types";
import { Database } from "./sqlite";
export let chinook: Database | undefined;
export let preload = (async function () {
  // TODO - maybe let the user put in their own url or upload a file?
  try {
    let res = await self.fetch("chinook.db");
    if (res.status === 200) {
      let data = await res.arrayBuffer();
      chinook = new Database(data); // FIXME wrap
      console.log('loaded', chinook)
    } else {
      console.error(
        `fetch of chinook.db got status ${res.status}: ${res.statusText}`
      );
    }
  } catch (e) {
    console.log(e);
  }
})();

let SAMPLES: [string,string][] = [
    ['Table List', 'select type, name, tbl_name from sqlite_master'],
    ['Artists',   'select artistid, name from artists'],
    ['Albums',  'select albumid, title from albums'],
    ['join2',  'select name, title from artists, albums where albums.artistid = artists.artistid'],
    ['join3',  'select artists.name, title, tracks.name from artists, albums, tracks where albums.artistid = artists.artistid and tracks.albumid = albums.albumid'],
    ['join4',  `select artists.name, title, tracks.name, genres.name
                from artists, albums, tracks, genres 
                where albums.artistid = artists.artistid and tracks.albumid = albums.albumid and tracks.genreid = genres.genreid`],
    ['great',  'select rowid, name from artists where rowid > 100'],
    ['less',   'select rowid, name from artists where rowid < 100'],
    ['leq',    'select rowid, name from artists where rowid <= 100'],
]

function mdline2nodes(s: string) {
  let cs: (VNode<any> | string)[] = [];
  let toks = s.matchAll(
    /\*\*(.*?)\*\*|\*(.*?)\*|_(.*?)_|!\[(.*?)\]\((.*?)\)|:(\w+):|[^*]+|\*/g
  );
  for (let tok of toks) {
    (tok[1] && cs.push(h("b", {}, tok[1]))) ||
      (tok[2] && cs.push(h("em", {}, tok[2]))) ||
      (tok[3] && cs.push(h("em", {}, tok[0].slice(1, -1)))) ||
      (tok[5] && cs.push(h("img", { src: tok[5], alt: tok[4] }))) ||
      (tok[6] && cs.push(h(Icon, { name: tok[6] }))) ||
      cs.push(tok[0]);
  }
  return cs;
}

function md2nodes(md: string) {
  let rval: VNode[] = [];
  let list: VNode[] | undefined;
  let table: VNode[] | undefined;
  let cell = 'th'
  for (let line of md.split("\n")) {
    if (line.startsWith("- ")) {
      if (!list) {
        list = [];
        rval.push(h("ul", {}, list));
      }
      list.push(h("li", {}, mdline2nodes(line.slice(2))));
      continue;
    }
    if (line.startsWith("|")) {
      if (!table) {
        table = [];
        cell = 'th';
        rval.push(h("table",{}, table))
      }
      let parts = line.split('|').slice(1)
      if (parts[0]?.trim().match(/^-+$/)) cell = 'td'
      else table.push(h("tr", {}, parts.map(t => h(cell,{},md2nodes(t)))))
      continue
    }
    list = undefined;
    table = undefined;
    if (line.startsWith("# ")) {
      rval.push(h("h2", {}, mdline2nodes(line.slice(2))));
    } else if (line.startsWith("## ")) {
      rval.push(h("h3", {}, mdline2nodes(line.slice(3))));
    } else {
      rval.push(h("div", {}, mdline2nodes(line)));
    }
  }
  return rval;
}

const iframe = document.createElement("iframe");
iframe.src = "frame.html";
iframe.style.display = "none";
document.body.appendChild(iframe);

interface QResult {
    names: string[]
    table: any[][]
    error?: string
}

function getName(expr: Expr) {
  if (expr[0] == 'QN') return expr[2]
  return ''+expr
}

async function build(src: string) {
  if (!chinook) return
  await preload
  try {
    let query = parser(src);
    let names = query.select.map(getName)
    let res = execute(chinook, src)
    let table: any[][] = []
    for (let row of res) {
      table.push(row)
    }
    state.result.value = {names, table}
  } catch (e) {
    console.error(e)
    state.result.value = {names: [], table: [], error: ''+e}
  }
}

const state = {
  result: signal<QResult>({names:[], table:[]}),
  dark: signal(false),
  editor: signal<AbstractEditor | null>(null),
  toast: signal(""),
};

if (window.matchMedia) {
  function checkDark(ev: { matches: boolean }) {
    if (ev.matches) {
      document.body.className = "dark";
      state.dark.value = true;
      state.editor.value?.setDark(true);
    } else {
      document.body.className = "light";
      state.dark.value = false;
      state.editor.value?.setDark(false);
    }
  }
  let query = window.matchMedia("(prefers-color-scheme: dark)");
  query.addEventListener("change", checkDark);
  checkDark(query);
}

async function loadFile(fn: string) {
  let sample = SAMPLES.find(x => x[0] == fn)
  if (sample) {
    state.editor.value!.setValue(sample[1]);
    build(sample[1])
  }
}

async function copyToClipboard(ev: Event) {
  ev.preventDefault();
  let src = state.editor.value!.getValue();
  let hash = `#code=${encodeURIComponent(src)}`;
  window.location.hash = hash;
  await navigator.clipboard.writeText(window.location.href);
  state.toast.value = "URL copied to clipboard";
  setTimeout(() => (state.toast.value = ""), 2_000);
}

document.addEventListener("keydown", async (ev) => {
  if ((ev.metaKey || ev.ctrlKey) && ev.code == "KeyS") copyToClipboard(ev);
});

function getSavedCode() {
  let value: string = localStorage.sqlCode || LOADING;
  let hash = window.location.hash;
  if (hash.startsWith('#code=')) {
    try {
      value = decodeURIComponent(hash.slice(6))
    } catch (e) {
      console.error(e);
    }
  }
  return value;
}

const LOADING = "select type, name, tbl_name from sqlite_master";

let value = getSavedCode();

interface EditorProps {
  initialValue: string;
}

const language: EditorDelegate = {
  async getEntry(word, _row, _col) {
    return undefined;
  },
  
  async lint(view) {
    let src = view.state.doc.toString();
    localStorage.sqlCode = src;
    let value = src;
    // TODO only parse here and run the query on "Play"
    await build(value);
    // FIXME markers for parse/name errors
    let markers: any[] = []
    try {
      let diags: Diagnostic[] = [];
      for (let marker of markers) {
        let col = marker.startColumn;

        let line = view.state.doc.line(marker.startLineNumber);
        const pos = line.from + col - 1;
        let word = view.state.wordAt(pos);
        diags.push({
          from: word?.from ?? pos,
          to: word?.to ?? pos + 1,
          severity: marker.severity,
          message: marker.message,
        });
      }
      return diags;
    } catch (e) {
      console.error(e);
    }
    return [];
  },
};

function Editor({ initialValue }: EditorProps) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const container = ref.current!;
    const editor = new CMEditor(container, value, language);
    state.editor.value = editor;
    editor.setDark(state.dark.value)

    if (initialValue === LOADING) loadFile("Main.idr");
    else build(initialValue);
  }, []);

  return h("div", { id: "editor", ref });
}

function Result() {
  const {names, table, error} = state.result.value
  if (error) return h('div', { class: 'error'}, error)
  return h('div', {id: 'result'}, h("table", { class: 'result' },
    h('tr', {}, names.map(x => h('th',{},x))),
    table.map(row => h('tr', {}, row.map(x => h('td', {}, ''+x))))
  ));
}

function Help() {
  return h("div", { id: "help" }, md2nodes(helpText));
}

const OUTPUT = "Output";
const HELP = "Help";

function Tabs() {
  const [selected, setSelected] = useState( localStorage.sqlTab ?? OUTPUT)
  const Tab = (label: string) => {
    let onClick = () => {
      setSelected(label);
      localStorage.sqlTab = label;
    };
    let className = "tab";
    if (label == selected) className += " selected";
    return h("div", { className, onClick }, label);
  };

  let body;
  switch (selected) {
    case OUTPUT:
      body = h(Result, { field: "output" });
      break;
    case HELP:
      body = h(Help, {});
      break;
    default:
      body = h("div", {});
  }

  return h(
    "div",
    { className: "tabPanel right" },
    h(
      "div",
      { className: "tabBar" },
      Tab(OUTPUT),
      Tab(HELP)
    ),
    h("div", { className: "tabBody" }, body)
  );
}

function Icon({name}: {name: string}) {
  return h('svg', {'class':'icon'}, h('use', {href:`#${name}`}))
}

function EditWrap() {
  const options = SAMPLES.map(([value,_]) => h("option", { value }, value));

  const onChange = async (ev: ChangeEvent) => {
    if (ev.target instanceof HTMLSelectElement) {
      let fn = ev.target.value;
      ev.target.value = "";
      loadFile(fn);
    }
  };
  return h(
    "div",
    { className: "tabPanel left" },
    h(
      "div",
      { className: "tabBar" },
      h(
        "select",
        { onChange },
        h("option", { value: "" }, "load sample"),
        options
      ),
      h("a", {href: 'https://github.com/dunhamsteve/sqljs', title: 'github', target:'_blank'}, Icon({name: "github"})),
      h("div", { style: { flex: "1 1" } }),
      h(
        "button",
        { onClick: copyToClipboard, title: "share" },
        Icon({ name: "share" })
      ),
    ),
    h(
      "div",
      { className: "tabBody editor" },
      h(Editor, { initialValue: value })
    )
  );
}

function App() {
  let toast;
  if (state.toast.value) {
    toast = h("p", { className: "toast" }, h("div", {}, state.toast.value));
  }
  return h(
    "div",
    { className: 'wrapper' },
    toast,
    h(EditWrap, { }),
    h(Tabs, {})
  );
}

render(h(App, {}), document.getElementById("app")!);
