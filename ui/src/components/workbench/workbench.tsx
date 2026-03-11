"use client";

/**
 * Workbench — main workbench shell with toggle button and animated panel.
 *
 * Port of bolt.new's Workbench.client.tsx, adapted for Next.js.
 * Uses @nanostores/react for store subscriptions and framer-motion for animation.
 */

import { memo, useCallback, useEffect, useMemo } from 'react';
import { useStore } from '@nanostores/react';
import { motion, type Variants } from 'framer-motion';
import { XCircle, ChevronLeft, ChevronRight, FileCode, Terminal } from 'lucide-react';
import { workbenchStore } from '@/lib/workbench-store';
import { cubicEasingFn } from '@/lib/utils/easings';
import { EditorPanel } from './editor-panel';

interface WorkbenchProps {
  chatStarted?: boolean;
  isStreaming?: boolean;
}

const workbenchVariants = {
  closed: {
    width: 0,
    transition: {
      duration: 0.2,
      ease: cubicEasingFn,
    },
  },
  open: {
    width: 'var(--workbench-width)',
    transition: {
      duration: 0.2,
      ease: cubicEasingFn,
    },
  },
} satisfies Variants;

export const Workbench = memo(({ chatStarted, isStreaming }: WorkbenchProps) => {
  const showWorkbench = useStore(workbenchStore.showWorkbench);
  const selectedFile = useStore(workbenchStore.selectedFile);
  const currentDocument = useStore(workbenchStore.currentDocument);
  const unsavedFiles = useStore(workbenchStore.unsavedFiles);
  const files = useStore(workbenchStore.files);

  useEffect(() => {
    workbenchStore.setDocuments(files);
  }, [files]);

  const onEditorChange = useCallback((update: { content: string }) => {
    workbenchStore.setCurrentDocumentContent(update.content);
  }, []);

  const onEditorScroll = useCallback((position: { top: number; left: number }) => {
    workbenchStore.setCurrentDocumentScrollPosition(position);
  }, []);

  const onFileSelect = useCallback((filePath: string | undefined) => {
    workbenchStore.setSelectedFile(filePath);
  }, []);

  const onFileSave = useCallback(() => {
    workbenchStore.saveCurrentDocument().catch(() => {
      // TODO: add toast notification for save failure
      console.error('Failed to save file');
    });
  }, []);

  const onFileReset = useCallback(() => {
    workbenchStore.resetCurrentDocument();
  }, []);

  const handleToggle = useCallback(() => {
    const current = workbenchStore.showWorkbench.get();
    workbenchStore.setShowWorkbench(!current);
  }, []);

  const handleClose = useCallback(() => {
    workbenchStore.setShowWorkbench(false);
  }, []);

  const fileCount = useMemo(() => {
    return Object.keys(files).filter((key) => files[key]?.type === 'file').length;
  }, [files]);

  if (!chatStarted) return null;

  return (
    <>
      {/* Toggle button */}
      <button
        onClick={handleToggle}
        className="fixed right-0 top-1/2 -translate-y-1/2 z-50 flex items-center gap-1 bg-[#1a1a1a] border border-white/20 border-r-0 rounded-l-lg px-2 py-3 text-white/70 hover:text-white hover:bg-[#252525] transition-colors"
        title={showWorkbench ? 'Hide Workbench' : 'Show Workbench'}
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
      <motion.div
        initial="closed"
        animate={showWorkbench ? 'open' : 'closed'}
        variants={workbenchVariants}
        className="z-workbench h-full shrink-0 overflow-hidden"
        style={{ pointerEvents: showWorkbench ? 'auto' : 'none' }}
      >
        <div className="h-full box-border py-3 pr-3 pl-1">
          <div className="h-full w-full">
            <div className="h-full flex flex-col bg-[#0d0d0d] border border-white/10 shadow-lg rounded-lg overflow-hidden">
              {/* Header */}
              <div className="flex items-center px-3 py-2 border-b border-white/10 bg-[#0a0a0a]">
                <span className="text-sm font-medium text-white/80">Workbench</span>
                <span className="ml-2 text-xs text-white/50">({fileCount} files)</span>
                <div className="ml-auto flex items-center gap-1">
                  <button
                    onClick={() => workbenchStore.toggleTerminal()}
                    className="flex items-center gap-1.5 rounded-md px-2 py-1.5 text-xs font-medium transition-colors text-white/60 hover:text-white hover:bg-white/5"
                    type="button"
                  >
                    <Terminal className="h-3.5 w-3.5" />
                    Toggle Terminal
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

              {/* Editor Panel — contains file tree, editor, and terminal */}
              <div className="relative flex-1 overflow-hidden">
                <EditorPanel
                  editorDocument={currentDocument}
                  isStreaming={isStreaming}
                  selectedFile={selectedFile}
                  files={files}
                  unsavedFiles={unsavedFiles}
                  onFileSelect={onFileSelect}
                  onEditorScroll={onEditorScroll}
                  onEditorChange={onEditorChange}
                  onFileSave={onFileSave}
                  onFileReset={onFileReset}
                />
              </div>
            </div>
          </div>
        </div>
      </motion.div>
    </>
  );
});

Workbench.displayName = 'Workbench';
