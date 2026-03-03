"use client";

import * as React from "react";
import { AnimatePresence, motion } from "framer-motion";
import { Terminal, XCircle, ChevronLeft, ChevronRight, FileCode } from "lucide-react";
import { workbenchStore, type EditorDocument } from "@/lib/workbench-store";
import { EditorPanel } from "./editor-panel";

interface WorkbenchProps {
  chatStarted?: boolean;
  isStreaming?: boolean;
}

/**
 * Generic hook for your store nodes that have:
 *   - get(): T
 *   - subscribe(cb): () => void
 */
function useStoreValue<T>(node: { get: () => T; subscribe: (cb: () => void) => () => void }): T {
  return React.useSyncExternalStore(node.subscribe, node.get, node.get);
}

export function Workbench({ chatStarted, isStreaming }: WorkbenchProps) {
  // ✅ Proper subscriptions (no manual forceUpdate needed)
  const showWorkbench = useStoreValue(workbenchStore.showWorkbench);
  const selectedFile = useStoreValue(workbenchStore.selectedFile);
  const files = useStoreValue(workbenchStore.files);
  const unsavedFiles = useStoreValue(workbenchStore.unsavedFiles);
  const showTerminal = useStoreValue(workbenchStore.showTerminal);

  // ✅ Refs to avoid stale closures + avoid putting large objects in deps
  const selectedFileRef = React.useRef<string | undefined>(selectedFile);
  const filesRef = React.useRef(files);
  const unsavedRef = React.useRef(unsavedFiles);

  React.useEffect(() => {
    selectedFileRef.current = selectedFile;
  }, [selectedFile]);

  React.useEffect(() => {
    filesRef.current = files;
  }, [files]);

  React.useEffect(() => {
    unsavedRef.current = unsavedFiles;
  }, [unsavedFiles]);

  // ✅ Show workbench by default when chat starts
  React.useEffect(() => {
    if (chatStarted && !workbenchStore.showWorkbench.get()) {
      workbenchStore.setShowWorkbench(true);
    }
  }, [chatStarted]);

  // ✅ Build editorDocument from subscribed `files`
  const editorDocument = React.useMemo((): EditorDocument | undefined => {
    if (!selectedFile) return undefined;
    const file = files[selectedFile];
    if (!file || file.type !== "file") return undefined;

    return {
      value: file.content ?? "",
      isBinary: !!file.isBinary,
      filePath: selectedFile,
    };
  }, [selectedFile, files]);

  const handleFileSelect = React.useCallback((filePath: string | undefined) => {
    workbenchStore.setSelectedFile(filePath);
  }, []);

  /**
   * ✅ CRITICAL FIX:
   * - Do not write to store if content didn't change (break feedback loop)
   * - Do not re-set unsavedFiles if already unsaved
   */
  const handleEditorChange = React.useCallback((content: string) => {
    const path = selectedFileRef.current;
    if (!path) return;

    const currentFiles = filesRef.current;
    const current = currentFiles[path];

    // ✅ No-op guard: if store already has the same content, don't set again
    if (current?.type === "file" && (current.content ?? "") === content) {
      return;
    }

    // Update file content
    workbenchStore.files.setKey(path, {
      type: "file",
      content,
      isBinary: false,
    });

    // Mark as unsaved (only if not already)
    const currentUnsaved = unsavedRef.current;
    if (!currentUnsaved.has(path)) {
      const next = new Set(currentUnsaved);
      next.add(path);
      workbenchStore.unsavedFiles.set(next);
    }
  }, []);

  const handleFileSave = React.useCallback(() => {
    workbenchStore.saveCurrentDocument();
  }, []);

  const handleFileReset = React.useCallback(() => {
    // Reset file content to last saved state
    workbenchStore.resetCurrentDocument();
  }, []);

  const handleToggleTerminal = React.useCallback(() => {
    workbenchStore.toggleTerminal();
  }, []);

  const handleClose = React.useCallback(() => {
    workbenchStore.setShowWorkbench(false);
  }, []);

  // ✅ Avoid depending on `showWorkbench` (stable, no stale closure)
  const handleToggle = React.useCallback(() => {
    const current = workbenchStore.showWorkbench.get();
    workbenchStore.setShowWorkbench(!current);
  }, []);

  const fileCount = React.useMemo(() => {
    return Object.keys(files).filter((key) => files[key]?.type === "file").length;
  }, [files]);

  if (!chatStarted) return null;

  return (
    <>
      {/* Toggle button */}
      <button
        onClick={handleToggle}
        className="fixed right-0 top-1/2 -translate-y-1/2 z-50 flex items-center gap-1 bg-[#1a1a1a] border border-white/20 border-r-0 rounded-l-lg px-2 py-3 text-white/70 hover:text-white hover:bg-[#252525] transition-colors"
        title={showWorkbench ? "Hide Workbench" : "Show Workbench"}
        type="button"
      >
        {showWorkbench ? (
          <ChevronRight className="h-4 w-4" />
        ) : (
          <ChevronLeft className="h-4 w-4" />
        )}
        <FileCode className="h-4 w-4" />
        {fileCount > 0 && !showWorkbench && (
          <span className="absolute -top-1 -left-1 flex h-4 w-4 items-center justify-center rounded-full bg-blue-500 text-[10px] font-bold text-white">
            {fileCount}
          </span>
        )}
      </button>

      {/* Workbench panel */}
      <AnimatePresence>
        {showWorkbench && (
          <motion.div
            initial={{ width: "0%", opacity: 0 }}
            animate={{ width: "50%", opacity: 1 }}
            exit={{ width: "0%", opacity: 0 }}
            transition={{ duration: 0.2, ease: [0.4, 0, 0.2, 1] }}
            className="z-40 h-full shrink-0 overflow-hidden"
          >
            <div className="h-full w-full box-border p-4 pl-0">
              <div className="h-full flex flex-col bg-[#0d0d0d] border border-white/10 shadow-lg rounded-lg overflow-hidden">
                {/* Header */}
                <div className="flex items-center px-3 py-2 border-b border-white/10 bg-[#0a0a0a]">
                  <span className="text-sm font-medium text-white/80">Workbench</span>
                  <span className="ml-2 text-xs text-white/50">({fileCount} files)</span>
                  <div className="ml-auto flex items-center gap-1">
                    <button
                      onClick={handleToggleTerminal}
                      className={cn(
                        "flex items-center gap-1.5 px-2.5 py-1.5 text-xs rounded-md transition-colors",
                        showTerminal
                          ? "bg-white/15 text-white"
                          : "bg-transparent text-white/60 hover:text-white hover:bg-white/10"
                      )}
                      type="button"
                    >
                      <Terminal className="h-3.5 w-3.5" />
                      Terminal
                    </button>
                    <button
                      onClick={handleClose}
                      className="p-1.5 rounded-md text-white/60 hover:text-white hover:bg-white/10"
                      type="button"
                    >
                      <XCircle className="h-4 w-4" />
                    </button>
                  </div>
                </div>

                {/* Editor Panel */}
                <div className="relative flex-1 overflow-hidden">
                  <EditorPanel
                    editorDocument={editorDocument}
                    isStreaming={isStreaming}
                    selectedFile={selectedFile}
                    files={files}
                    unsavedFiles={unsavedFiles}
                    onFileSelect={handleFileSelect}
                    onEditorChange={handleEditorChange}
                    onFileSave={handleFileSave}
                    onFileReset={handleFileReset}
                    showTerminal={showTerminal}
                    onToggleTerminal={handleToggleTerminal}
                  />
                </div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}

// Helper function (since we can't import from utils)
function cn(...classes: (string | boolean | undefined | null)[]) {
  return classes.filter(Boolean).join(" ");
}
