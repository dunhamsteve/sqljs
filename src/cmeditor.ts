import { AbstractEditor, EditorDelegate, Marker } from "./types";
import { basicSetup } from "codemirror";
import { indentMore, indentLess, toggleLineComment } from "@codemirror/commands";
import { EditorView, keymap } from "@codemirror/view";
import { Compartment, Prec } from "@codemirror/state";
import { oneDark } from "@codemirror/theme-one-dark";
import { linter } from "@codemirror/lint";
import { LanguageSupport, StreamLanguage, StringStream } from "@codemirror/language";
import { Diagnostic } from "@codemirror/lint";

interface FC {
  file: string;
  line: number;
  col: number;
}

interface TopEntry {
  fc: FC;
  name: string;
  type: string;
}
export interface EditorDelegate {
  getEntry(word: string, row: number, col: number): Promise<TopEntry | undefined>
  lint(view: EditorView): Promise<Diagnostic[]> | Diagnostic[]
}
export interface Marker {
  severity: 'error' | 'info' | 'warning'
  message: string
  startColumn: number
  startLineNumber: number
  endColumn: number
  endLineNumber: number
}
export interface AbstractEditor {
  setValue: (_: string) => unknown;
  getValue: () => string;
  setMarkers: (_: Marker[]) => unknown
  setDark(isDark: boolean): unknown
}


// maybe use https://github.com/codemirror/legacy-modes/blob/main/mode/simple-mode.js instead.
// @codemirror/legacy-modes/mode/simple-mode.js

const keywords = "select as on from left outer join where and or null".split(' ');

// see https://lezer.codemirror.net/docs/ref/#highlight.Tag%5EdefineModifier for tag list
function tokenizer(stream: StringStream, state: unknown): string | null {
  if (stream.eatSpace()) return null;
  if (stream.match("--")) {
    stream.skipToEnd();
    return "comment";
  }
  if (stream.match(/\w+/)) {
    let word = stream.current();
    if (keywords.includes(word.toLowerCase())) return "keyword";
    return "variableName";
  }
  // unhandled char
  stream.next();
  return null;
}

const sqlLanguage = StreamLanguage.define({
  // startState: () => ({ tokenizers: [tokenizer] }),
  token: tokenizer,
  languageData: {
    commentTokens: {
      line: "--",
    },
  },
});

export class CMEditor implements AbstractEditor {
  view: EditorView;
  delegate: EditorDelegate;
  theme: Compartment;
  constructor(container: HTMLElement, doc: string, delegate: EditorDelegate) {
    this.delegate = delegate;
    this.theme = new Compartment();
    this.view = new EditorView({
      doc,
      parent: container,
      extensions: [
        basicSetup,
        linter((view) => this.delegate.lint(view)),
        Prec.highest(keymap.of([
          { key: "Tab", preventDefault: true, run: indentMore },
          {
            key: "Shift-Tab",
            preventDefault: true,
            run: indentLess,
          },
          { key: "Cmd-/", run: toggleLineComment },
        ])),
        this.theme.of(EditorView.baseTheme({})),
        // hoverTooltip(async (view, pos) => {
        //   let cursor = this.view.state.doc.lineAt(pos);
        //   let line = cursor.number;
        //   let range = this.view.state.wordAt(pos);
        //   console.log(range);
        //   if (range) {
        //     let col = range.from - cursor.from;
        //     let word = this.view.state.doc.sliceString(range.from, range.to);
        //     let entry = await this.delegate.getEntry(word, line, col);
        //     console.log("entry for", word, "is", entry);
        //     if (entry) {
        //       let rval: Tooltip = {
        //         pos: range.head,
        //         above: true,
        //         create: () => {
        //           let dom = document.createElement("div");
        //           dom.className = "tooltip";
        //           dom.textContent = entry.type;
        //           return { dom };
        //         },
        //       };
        //       return rval;
        //     }
        //   }
        //   // we'll iterate the syntax tree for word.
        //   // let entry = delegate.getEntry(word, line, col)
        //   return null;
        // }),
        new LanguageSupport(sqlLanguage),
      ],
    });
  }
  setDark(isDark: boolean) {
    this.view.dispatch({
      effects: this.theme.reconfigure(
        isDark ? oneDark : EditorView.baseTheme({})
      ),
    });
  }
  setValue(_doc: string) {
    this.view.dispatch({
      changes: { from: 0, to: this.view.state.doc.length, insert: _doc },
    });
  }
  getValue() {
    return this.view.state.doc.toString();
  }
  setMarkers(_: Marker[]) { }
}
