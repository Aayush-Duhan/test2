"use client";

import React, { memo, useEffect, useMemo, useRef, useState } from "react";

import { autocompletion, closeBrackets, closeBracketsKeymap } from "@codemirror/autocomplete";
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import {
  bracketMatching,
  foldGutter,
  foldKeymap,
  indentOnInput,
  indentUnit,
} from "@codemirror/language";
import { highlightSelectionMatches, searchKeymap } from "@codemirror/search";
import {
  Compartment,
  EditorSelection,
  EditorState,
  Transaction,
} from "@codemirror/state";
import {
  crosshairCursor,
  drawSelection,
  dropCursor,
  EditorView,
  highlightActiveLine,
  highlightActiveLineGutter,
  highlightSpecialChars,
  keymap,
  lineNumbers,
  placeholder,
  rectangularSelection,
  scrollPastEnd,
  ViewPlugin,
  type ViewUpdate,
} from "@codemirror/view";

import { cn } from "@/lib/utils";
import { getSyntaxTheme, getTheme, type EditorSettings } from "./cm-theme";
import { getLanguage } from "./languages";

export interface EditorDocument {
  value: string;
  isBinary: boolean;
  filePath: string;
}

export interface ScrollPosition {
  top: number;
  left: number;
}

export interface EditorUpdate {
  selection: EditorSelection;
  content: string;
}

export type OnChangeCallback = (update: EditorUpdate) => void;
export type OnScrollCallback = (position: ScrollPosition) => void;
export type OnSaveCallback = () => void;

interface Props {
  id?: unknown;
  doc?: EditorDocument;
  editable?: boolean;
  onChange?: OnChangeCallback;
  onScroll?: OnScrollCallback;
  onSave?: OnSaveCallback;
  className?: string;
  settings?: EditorSettings;

  /** UX extras */
  placeholderText?: string;
  /**
   * Debounces editor -> parent updates.
   * If you debounce in parent, set this to 0.
   */
  debounceMs?: number;
}

export const CodeMirrorEditor = memo(function CodeMirrorEditor({
  id,
  doc,
  editable = true,
  onChange,
  onScroll,
  onSave,
  className = "",
  settings,
  placeholderText = "Start typing…",
  debounceMs = 150,
}: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);

  // Keep latest callbacks without forcing editor reconfiguration
  const onChangeRef = useRef(onChange);
  const onSaveRef = useRef(onSave);
  const onScrollRef = useRef(onScroll);
  const currentFilePathRef = useRef<string | undefined>(doc?.filePath);

  useEffect(() => {
    onChangeRef.current = onChange;
    onSaveRef.current = onSave;
    onScrollRef.current = onScroll;
  });

  useEffect(() => {
    currentFilePathRef.current = doc?.filePath;
  }, [doc?.filePath]);

  // Compartments allow reconfiguration without recreating the view
  const [languageCompartment] = useState(() => new Compartment());
  const [editableCompartment] = useState(() => new Compartment());
  const [themeCompartment] = useState(() => new Compartment());
  const [placeholderCompartment] = useState(() => new Compartment());

  const [editorView, setEditorView] = useState<EditorView | null>(null);

  // Prevent feedback loop: track what the editor currently has
  const lastAppliedValueRef = useRef<string>("");

  // Preserve selection + scroll per file (great UX for tab/file switching)
  const selectionByFileRef = useRef<Map<string, EditorSelection>>(new Map());
  const scrollByFileRef = useRef<Map<string, ScrollPosition>>(new Map());

  // Debounce outgoing onChange
  const changeTimerRef = useRef<number | null>(null);

  const scrollPlugin = useMemo(() => {
    return ViewPlugin.fromClass(
      class {
        lastTop = -1;
        lastLeft = -1;

        update(update: ViewUpdate) {
          const view = update.view;
          const top = view.scrollDOM.scrollTop;
          const left = view.scrollDOM.scrollLeft;

          if (top !== this.lastTop || left !== this.lastLeft) {
            this.lastTop = top;
            this.lastLeft = left;

            // Store per filePath
            const filePath = currentFilePathRef.current;
            if (filePath) {
              scrollByFileRef.current.set(filePath, { top, left });
            }

            onScrollRef.current?.({ top, left });
          }
        }
      }
    );
  }, []);

  // ✅ Create EditorView exactly once
  useEffect(() => {
    if (!containerRef.current) return;

    const updateListener = EditorView.updateListener.of((update) => {
      const filePath = currentFilePathRef.current;
      if (filePath && (update.docChanged || update.selectionSet)) {
        selectionByFileRef.current.set(filePath, update.state.selection);
      }
      if (!update.docChanged) return;

      const content = update.state.doc.toString();
      lastAppliedValueRef.current = content;

      // Debounced notify parent
      if (onChangeRef.current) {
        if (changeTimerRef.current) window.clearTimeout(changeTimerRef.current);

        if (debounceMs > 0) {
          changeTimerRef.current = window.setTimeout(() => {
            onChangeRef.current?.({
              selection: update.state.selection,
              content,
            });
          }, debounceMs);
        } else {
          onChangeRef.current?.({
            selection: update.state.selection,
            content,
          });
        }
      }
    });

    const saveKeyBinding = keymap.of([
      {
        key: "Mod-s",
        run: () => {
          onSaveRef.current?.();
          return true;
        },
      },
    ]);

    const state = EditorState.create({
      doc: doc?.value ?? "",
      extensions: [
        // --- Core editor UX ---
        lineNumbers(),
        highlightActiveLineGutter(),
        highlightActiveLine(),
        highlightSpecialChars(),
        drawSelection(),
        dropCursor(),
        rectangularSelection(),
        crosshairCursor(),

        // --- History & code behavior ---
        history(),
        indentOnInput(),
        indentUnit.of("  "),
        bracketMatching(),
        closeBrackets(),
        autocompletion({ activateOnTyping: true }),
        foldGutter(),
        scrollPastEnd(),

        // --- Search UX ---
        highlightSelectionMatches(),
        getSyntaxTheme(),

        // --- Input attributes (code-friendly) ---
        EditorView.contentAttributes.of({
          spellcheck: "false",
          autocapitalize: "off",
          autocomplete: "off",
          autocorrect: "off",
        }),

        // --- Keymaps ---
        keymap.of([
          ...defaultKeymap,
          ...historyKeymap,
          ...searchKeymap,
          ...closeBracketsKeymap,
          ...foldKeymap,
        ]),
        saveKeyBinding,

        // --- Allow multi-cursor (nice UX) ---
        EditorState.allowMultipleSelections.of(true),

        // --- Plugins ---
        updateListener,
        scrollPlugin,

        // --- Compartments ---
        languageCompartment.of([]),
        editableCompartment.of(EditorView.editable.of(editable)),
        themeCompartment.of(getTheme(settings)),
        placeholderCompartment.of(placeholder(placeholderText)),
      ],
    });

    const view = new EditorView({
      state,
      parent: containerRef.current,
    });

    lastAppliedValueRef.current = view.state.doc.toString();
    setEditorView(view);

    return () => {
      if (changeTimerRef.current) window.clearTimeout(changeTimerRef.current);
      view.destroy();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ✅ Update editable without recreating editor
  useEffect(() => {
    if (!editorView) return;
    editorView.dispatch({
      effects: editableCompartment.reconfigure(EditorView.editable.of(editable)),
    });
  }, [editable, editorView, editableCompartment]);

  // ✅ Update theme without recreating editor
  useEffect(() => {
    if (!editorView) return;
    editorView.dispatch({
      effects: themeCompartment.reconfigure(getTheme(settings)),
    });
  }, [settings, editorView, themeCompartment]);

  // ✅ Update placeholder
  useEffect(() => {
    if (!editorView) return;
    editorView.dispatch({
      effects: placeholderCompartment.reconfigure(placeholder(placeholderText)),
    });
  }, [placeholderText, editorView, placeholderCompartment]);

  // ✅ Load language only when filePath changes
  useEffect(() => {
    if (!editorView || !doc?.filePath) return;

    let cancelled = false;

    (async () => {
      const language = await getLanguage(doc.filePath);
      if (cancelled) return;

      editorView.dispatch({
        effects: languageCompartment.reconfigure(language ?? []),
      });
    })();

    return () => {
      cancelled = true;
    };
  }, [doc?.filePath, editorView, languageCompartment]);

  // ✅ External content sync (prevents cursor jump, preserves selection & scroll, no undo pollution)
  const docValue = doc?.value;
  const docFilePath = doc?.filePath;

  useEffect(() => {
    if (!editorView || !docFilePath) return;

    const incoming = docValue ?? "";
    const current = editorView.state.doc.toString();

    // Avoid feedback loop & unnecessary dispatch
    if (incoming === current) return;

    // Preserve current scroll
    const currentTop = editorView.scrollDOM.scrollTop;
    const currentLeft = editorView.scrollDOM.scrollLeft;

    // Restore per-file selection/scroll if we have it
    const savedSel = selectionByFileRef.current.get(docFilePath);
    const savedScroll = scrollByFileRef.current.get(docFilePath);

    const selectionToRestore = savedSel ?? editorView.state.selection;

    editorView.dispatch({
      changes: { from: 0, to: editorView.state.doc.length, insert: incoming },
      selection: clampSelection(selectionToRestore, incoming.length),
      annotations: Transaction.addToHistory.of(false), // external sync shouldn't affect undo
    });

    lastAppliedValueRef.current = incoming;

    // Restore scroll:
    // - If switching file, prefer saved scroll
    // - Otherwise keep current scroll position
    const next = savedScroll ?? { top: currentTop, left: currentLeft };
    editorView.scrollDOM.scrollTop = next.top;
    editorView.scrollDOM.scrollLeft = next.left;
  }, [docValue, docFilePath, editorView]);

  return (
    <div
      ref={containerRef}
      className={cn("h-full w-full overflow-hidden", className)}
      data-id={id}
      aria-label="Code editor"
    />
  );
});

function clampSelection(sel: EditorSelection, docLen: number) {
  const clampPos = (pos: number) => Math.max(0, Math.min(pos, docLen));
  const ranges = sel.ranges.map((r) => EditorSelection.range(clampPos(r.anchor), clampPos(r.head)));
  return EditorSelection.create(ranges, sel.mainIndex);
}
