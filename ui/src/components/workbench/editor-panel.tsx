"use client";

import * as React from "react";
import {
  FolderTree,
  Save,
  RotateCcw,
  Terminal as TerminalIcon,
  ChevronDown,
  FileCode,
} from "lucide-react";

import type { FileMap, EditorDocument, TerminalLine } from "@/lib/workbench-store";
import { cn } from "@/lib/utils";
import { FileTree } from "./file-tree";
import { CodeMirrorEditor } from "./codemirror/CodeMirrorEditor";

interface EditorPanelProps {
  files?: FileMap;
  unsavedFiles?: Set<string>;
  editorDocument?: EditorDocument;
  selectedFile?: string;
  isStreaming?: boolean;
  terminalLines?: TerminalLine[];
  onFileSelect?: (value?: string) => void;
  onEditorChange?: (content: string) => void;
  onFileSave?: () => void;
  onFileReset?: () => void;
  showTerminal?: boolean;
  onToggleTerminal?: () => void;
}

export function EditorPanel({
  files,
  unsavedFiles,
  editorDocument,
  selectedFile,
  isStreaming,
  terminalLines = [],
  onFileSelect,
  onEditorChange,
  onFileSave,
  onFileReset,
  showTerminal = false,
  onToggleTerminal,
}: EditorPanelProps) {
  const filePath = editorDocument?.filePath;

  const activeFileSegments = React.useMemo(() => {
    if (!filePath) return undefined;
    return filePath.split("/");
  }, [filePath]);

  const activeFileUnsaved = React.useMemo(() => {
    return !!filePath && !!unsavedFiles?.has(filePath);
  }, [filePath, unsavedFiles]);

  // ✅ Memoize settings so CodeMirror doesn't reconfigure on every render
  const editorSettings = React.useMemo(
    () => ({
      fontSize: "13px",
      tabSize: 2,
    }),
    []
  );

  // ✅ Debounce store updates to reduce rerender pressure
  const changeTimer = React.useRef<number | null>(null);

  const handleEditorChange = React.useCallback(
    (update: { content: string }) => {
      if (!onEditorChange) return;

      if (changeTimer.current) window.clearTimeout(changeTimer.current);
      changeTimer.current = window.setTimeout(() => {
        onEditorChange(update.content);
      }, 120);
    },
    [onEditorChange]
  );

  React.useEffect(() => {
    return () => {
      if (changeTimer.current) window.clearTimeout(changeTimer.current);
    };
  }, []);

  const handleEditorSave = React.useCallback(() => {
    if (isStreaming) return;
    onFileSave?.();
  }, [onFileSave, isStreaming]);

  const canSaveOrReset = !!editorDocument && activeFileUnsaved && !isStreaming;

  return (
    <div className="flex h-full flex-col">
      {/* Main content area with file tree and editor */}
      <div className="flex flex-1 min-h-0">
        {/* File tree sidebar */}
        <div className="w-56 min-w-48 border-r border-white/10 flex flex-col bg-[#0d0d0d]">
          <div className="flex items-center gap-2 px-3 py-2 border-b border-white/10 text-xs font-medium text-white/70">
            <FolderTree className="h-3.5 w-3.5" />
            Files
          </div>
          <div className="flex-1 overflow-y-auto">
            <FileTree
              className="h-full py-1"
              files={files}
              hideRoot
              unsavedFiles={unsavedFiles}
              rootFolder="/project"
              selectedFile={selectedFile}
              onFileSelect={onFileSelect}
            />
          </div>
        </div>

        {/* Editor area */}
        <div className="flex-1 flex flex-col min-w-0">
          {/* Breadcrumb header */}
          <div className="flex items-center gap-2 px-3 py-2 border-b border-white/10 bg-[#0d0d0d] overflow-x-auto">
            {activeFileSegments?.length ? (
              <div className="flex items-center gap-2 text-sm text-white/70 min-w-0 w-full">
                <FileCode className="h-3.5 w-3.5 shrink-0" />

                <div className="flex items-center gap-1 min-w-0">
                  {activeFileSegments.map((segment, index) => (
                    <React.Fragment key={index}>
                      {index > 0 && <span className="text-white/40">/</span>}
                      <span
                        className={cn(
                          "truncate max-w-32",
                          index === activeFileSegments.length - 1 ? "text-white" : "text-white/60"
                        )}
                        title={segment}
                      >
                        {segment}
                      </span>
                    </React.Fragment>
                  ))}
                </div>

                {/* Right actions */}
                <div className="ml-auto flex items-center gap-2 shrink-0">
                  {isStreaming && (
                    <span className="text-xs px-2 py-1 rounded bg-white/10 text-white/70">
                      Streaming…
                    </span>
                  )}

                  {activeFileUnsaved && (
                    <span className="text-xs text-yellow-300/90" title="Unsaved changes">
                      ●
                    </span>
                  )}

                  <button
                    onClick={onFileSave}
                    disabled={!canSaveOrReset}
                    className={cn(
                      "flex items-center gap-1 px-2 py-1 text-xs rounded",
                      canSaveOrReset
                        ? "hover:bg-white/10 text-white/70 hover:text-white"
                        : "opacity-40 cursor-not-allowed"
                    )}
                  >
                    <Save className="h-3 w-3" />
                    Save
                  </button>

                  <button
                    onClick={onFileReset}
                    disabled={!canSaveOrReset}
                    className={cn(
                      "flex items-center gap-1 px-2 py-1 text-xs rounded",
                      canSaveOrReset
                        ? "hover:bg-white/10 text-white/70 hover:text-white"
                        : "opacity-40 cursor-not-allowed"
                    )}
                  >
                    <RotateCcw className="h-3 w-3" />
                    Reset
                  </button>
                </div>
              </div>
            ) : (
              <span className="text-sm text-white/40">Select a file to view</span>
            )}
          </div>

          {/* Code editor */}
          <div className="flex-1 overflow-hidden bg-[#0a0a0a] relative">
            {editorDocument ? (
              editorDocument.isBinary ? (
                <div className="flex items-center justify-center h-full text-white/50">
                  <p>Binary file - cannot display</p>
                </div>
              ) : (
                <>
                  <CodeMirrorEditor
                    doc={editorDocument}
                    editable={!isStreaming}
                    onChange={handleEditorChange}
                    onSave={handleEditorSave}
                    settings={editorSettings}
                    placeholderText="Type here…"
                    debounceMs={0} // Debounce already handled in panel
                  />

                  {/* Optional overlay while streaming */}
                  {isStreaming && (
                    <div className="absolute inset-0 pointer-events-none bg-black/10" />
                  )}
                </>
              )
            ) : (
              <div className="flex items-center justify-center h-full text-white/40">
                <p>No file selected</p>
              </div>
            )}
          </div>
        </div>
      </div>

      {showTerminal && (
        <div className="h-56 border-t border-white/10 bg-[#0a0a0a] flex flex-col">
          <div className="flex items-center gap-2 px-3 py-2 border-b border-white/10 bg-[#0d0d0d]">
            <div className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-full bg-white/10 text-white">
              <TerminalIcon className="h-3.5 w-3.5" />
              AI Agent
            </div>
            <button
              onClick={onToggleTerminal}
              className="ml-auto p-1 rounded hover:bg-white/10 text-white/60 hover:text-white"
            >
              <ChevronDown className="h-4 w-4" />
            </button>
          </div>
          <TerminalOutput lines={terminalLines} />
        </div>
      )}
    </div>
  );
}

function TerminalOutput({ lines }: { lines: TerminalLine[] }) {
  const scrollRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    const el = scrollRef.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
    }
  }, [lines]);

  return (
    <div ref={scrollRef} className="flex-1 overflow-y-auto p-3 font-mono text-xs leading-relaxed">
      {lines.length === 0 ? (
        <div className="text-white/30">Waiting for agent commands...</div>
      ) : (
        lines.map((line, i) => (
          <div
            key={`${i}-${line.ts}`}
            className={line.isProgress ? "text-emerald-400/90" : "text-white/70"}
          >
            {line.text}
          </div>
        ))
      )}
    </div>
  );
}