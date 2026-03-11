"use client";

/**
 * EditorPanel — resizable editor + file tree + terminal layout.
 *
 * Port of bolt.new's EditorPanel using react-resizable-panels for:
 * - Horizontal: file tree ↔ editor (resizable)
 * - Vertical: editor ↔ terminal (resizable, collapsible)
 */

import { useStore } from '@nanostores/react';
import { memo, useCallback, useEffect, useMemo, useRef } from 'react';
import {
  Panel,
  Group,
  Separator,
  usePanelRef,
  type PanelImperativeHandle,
} from 'react-resizable-panels';
import {
  FolderTree,
  Save,
  RotateCcw,
  Terminal as TerminalIcon,
  ChevronDown,
} from 'lucide-react';
import type { FileMap } from '@/lib/stores/files-store';
import type { EditorDocument } from '@/lib/stores/editor-store';
import { workbenchStore } from '@/lib/workbench-store';
import { cn } from '@/lib/utils';
import { FileBreadcrumb } from './file-breadcrumb';
import { FileTree } from './file-tree';
import { TerminalPane, type TerminalRef } from './terminal-pane';
import { CodeMirrorEditor } from './codemirror/CodeMirrorEditor';

interface EditorPanelProps {
  files?: FileMap;
  unsavedFiles?: Set<string>;
  editorDocument?: EditorDocument;
  selectedFile?: string;
  isStreaming?: boolean;
  onEditorChange?: (update: { content: string }) => void;
  onEditorScroll?: (position: { top: number; left: number }) => void;
  onFileSelect?: (value?: string) => void;
  onFileSave?: () => void;
  onFileReset?: () => void;
}

const DEFAULT_TERMINAL_SIZE = 25;
const DEFAULT_EDITOR_SIZE = 100 - DEFAULT_TERMINAL_SIZE;

const editorSettings = { tabSize: 2, fontSize: '13px' };

export const EditorPanel = memo(
  ({
    files,
    unsavedFiles,
    editorDocument,
    selectedFile,
    isStreaming,
    onFileSelect,
    onEditorChange,
    onEditorScroll,
    onFileSave,
    onFileReset,
  }: EditorPanelProps) => {
    const showTerminal = useStore(workbenchStore.showTerminal);

    const terminalRef = useRef<TerminalRef | null>(null);
    const terminalPanelRef = usePanelRef();
    const terminalToggledByShortcut = useRef(false);

    const activeFileSegments = useMemo(() => {
      if (!editorDocument) {
        return undefined;
      }
      return editorDocument.filePath.split('/');
    }, [editorDocument]);

    const activeFileUnsaved = useMemo(() => {
      return editorDocument !== undefined && unsavedFiles?.has(editorDocument.filePath);
    }, [editorDocument, unsavedFiles]);

    useEffect(() => {
      const terminal = terminalPanelRef.current;

      if (!terminal) {
        return;
      }

      const isCollapsed = terminal.isCollapsed();

      if (!showTerminal && !isCollapsed) {
        terminal.collapse();
      } else if (showTerminal && isCollapsed) {
        terminal.resize(DEFAULT_TERMINAL_SIZE);
      }

      terminalToggledByShortcut.current = false;
    }, [showTerminal, terminalPanelRef]);

    const handleEditorChange = useCallback(
      (update: { content: string }) => {
        onEditorChange?.(update);
      },
      [onEditorChange],
    );

    const handleEditorSave = useCallback(() => {
      if (isStreaming) return;
      onFileSave?.();
    }, [onFileSave, isStreaming]);

    const canSaveOrReset = !!editorDocument && activeFileUnsaved && !isStreaming;

    return (
      <Group orientation="vertical">
        <Panel defaultSize={showTerminal ? DEFAULT_EDITOR_SIZE : 100} minSize={20}>
          <Group orientation="horizontal">
            <Panel defaultSize={20} minSize={10} collapsible>
              <div className="flex flex-col border-r border-white/10 h-full bg-[#0d0d0d]">
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
            </Panel>
            <Separator />
            <Panel className="flex flex-col" defaultSize={80} minSize={20}>
              {/* Breadcrumb header */}
              <div className="flex items-center gap-2 px-3 py-2 border-b border-white/10 bg-[#0d0d0d] overflow-x-auto">
                {activeFileSegments?.length ? (
                  <div className="flex items-center flex-1 text-sm">
                    <FileBreadcrumb
                      pathSegments={activeFileSegments}
                      files={files}
                      onFileSelect={onFileSelect}
                    />
                    {activeFileUnsaved && (
                      <div className="flex gap-1 ml-auto -mr-1.5">
                        <button
                          onClick={onFileSave}
                          disabled={!canSaveOrReset}
                          className={cn(
                            'flex items-center gap-1 px-2 py-1 text-xs rounded',
                            canSaveOrReset
                              ? 'hover:bg-white/10 text-white/70 hover:text-white'
                              : 'opacity-40 cursor-not-allowed',
                          )}
                          type="button"
                        >
                          <Save className="h-3 w-3" />
                          Save
                        </button>
                        <button
                          onClick={onFileReset}
                          disabled={!canSaveOrReset}
                          className={cn(
                            'flex items-center gap-1 px-2 py-1 text-xs rounded',
                            canSaveOrReset
                              ? 'hover:bg-white/10 text-white/70 hover:text-white'
                              : 'opacity-40 cursor-not-allowed',
                          )}
                          type="button"
                        >
                          <RotateCcw className="h-3 w-3" />
                          Reset
                        </button>
                      </div>
                    )}
                  </div>
                ) : (
                  <span className="text-sm text-white/40">Select a file to view</span>
                )}
              </div>
              {/* Code editor */}
              <div className="h-full flex-1 overflow-hidden bg-[#0a0a0a] relative">
                {editorDocument ? (
                  editorDocument.isBinary ? (
                    <div className="flex items-center justify-center h-full text-white/50">
                      <p>Binary file — cannot display</p>
                    </div>
                  ) : (
                    <>
                      <CodeMirrorEditor
                        doc={editorDocument}
                        editable={!isStreaming && editorDocument !== undefined}
                        settings={editorSettings}
                        onChange={handleEditorChange}
                        onSave={handleEditorSave}
                        placeholderText="Type here…"
                        debounceMs={0}
                      />
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
            </Panel>
          </Group>
        </Panel>
        <Separator />
        <Panel
          panelRef={terminalPanelRef}
          defaultSize={showTerminal ? DEFAULT_TERMINAL_SIZE : 0}
          minSize={10}
          collapsible
          onResize={(panelSize) => {
            const isCollapsed = panelSize.asPercentage < 1;
            if (!terminalToggledByShortcut.current) {
              if (isCollapsed) {
                workbenchStore.toggleTerminal(false);
              } else {
                workbenchStore.toggleTerminal(true);
              }
            }
          }}
        >
          <div className="h-full">
            <div className="bg-[#0a0a0a] h-full flex flex-col">
              <div className="flex items-center border-y border-white/10 gap-1.5 min-h-[34px] p-2 bg-[#0d0d0d]">
                <button
                  className="flex items-center text-sm cursor-pointer gap-1.5 px-3 py-1.5 h-full whitespace-nowrap rounded-full bg-white/10 text-white/90"
                  type="button"
                >
                  <TerminalIcon className="h-3.5 w-3.5" />
                  AI Agent
                </button>
                <button
                  className="ml-auto p-1.5 rounded-md text-white/60 hover:text-white hover:bg-white/10"
                  title="Close"
                  type="button"
                  onClick={() => workbenchStore.toggleTerminal(false)}
                >
                  <ChevronDown className="h-4 w-4" />
                </button>
              </div>
              <TerminalPane
                key="agent-terminal"
                className="h-full overflow-hidden"
                ref={terminalRef}
                readonly
              />
            </div>
          </div>
        </Panel>
      </Group>
    );
  },
);

EditorPanel.displayName = 'EditorPanel';
