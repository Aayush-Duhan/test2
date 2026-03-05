/**
 * Workbench Store - Manages files uploaded by user and editor state
 * Simplified version adapted from bolt.new for the migration UI
 */

import { atom, map, type MapStore, type ReadableAtom, type WritableAtom } from 'nanostores';
import type { TerminalEvent } from '@/lib/chat-types';

// Types
export interface File {
  type: 'file';
  content: string;
  isBinary: boolean;
}

export interface Folder {
  type: 'folder';
}

type Dirent = File | Folder;

export type FileMap = Record<string, Dirent | undefined>;

export interface EditorDocument {
  value: string;
  isBinary: boolean;
  filePath: string;
}

export interface UploadedFile {
  name: string;
  content: string;
  relativePath?: string;
  isBinary?: boolean;
}

const WORK_DIR = '/project';

/**
 * WorkbenchStore class - manages file state and editor selection
 */
export class WorkbenchStore {
  #files: MapStore<FileMap> = map({});
  #savedFiles: Map<string, string> = new Map(); // Tracks original/saved content for reset
  #selectedFile: WritableAtom<string | undefined> = atom(undefined);
  #unsavedFiles: WritableAtom<Set<string>> = atom(new Set<string>());
  #showWorkbench: WritableAtom<boolean> = atom(false);
  #showTerminal: WritableAtom<boolean> = atom(false);
  #terminalEvents: WritableAtom<TerminalEvent[]> = atom([]);
  #size = 0;

  get files(): MapStore<FileMap> {
    return this.#files;
  }

  get selectedFile(): ReadableAtom<string | undefined> {
    return this.#selectedFile;
  }

  get unsavedFiles(): WritableAtom<Set<string>> {
    return this.#unsavedFiles;
  }

  get showWorkbench(): WritableAtom<boolean> {
    return this.#showWorkbench;
  }

  get showTerminal(): WritableAtom<boolean> {
    return this.#showTerminal;
  }

  get terminalEvents(): WritableAtom<TerminalEvent[]> {
    return this.#terminalEvents;
  }

  get filesCount(): number {
    return this.#size;
  }

  getFile(filePath: string): File | undefined {
    const dirent = this.#files.get()[filePath];
    if (dirent?.type !== 'file') {
      return undefined;
    }
    return dirent;
  }

  setShowWorkbench(show: boolean): void {
    this.#showWorkbench.set(show);
  }

  toggleTerminal(value?: boolean): void {
    this.#showTerminal.set(value ?? !this.#showTerminal.get());
  }

  appendTerminalEvents(events: TerminalEvent[]): void {
    if (!Array.isArray(events) || events.length === 0) return;
    this.#terminalEvents.set([...this.#terminalEvents.get(), ...events]);
  }

  setTerminalEvents(events: TerminalEvent[]): void {
    this.#terminalEvents.set(Array.isArray(events) ? events : []);
  }

  clearTerminalEvents(): void {
    this.#terminalEvents.set([]);
  }

  setSelectedFile(filePath: string | undefined): void {
    this.#selectedFile.set(filePath);
  }

  /**
   * Add uploaded files to the workbench
   */
  addUploadedFiles(files: UploadedFile[]): void {
    const hadFiles = this.#size > 0;
    const previousSelection = this.#selectedFile.get();

    for (const file of files) {
      const normalizedPath = this.#normalizePath(file.relativePath ?? file.name);
      if (!normalizedPath) continue;

      const existing = this.#files.get()[normalizedPath];
      if (existing?.type !== 'file') {
        this.#size++;
      }

      this.#files.setKey(normalizedPath, {
        type: 'file',
        content: file.content,
        isBinary: file.isBinary ?? false,
      });

      // Save original content for reset functionality
      this.#savedFiles.set(normalizedPath, file.content);
    }

    const currentFiles = this.#files.get();

    // Keep current selection when valid; otherwise select first available file.
    if (previousSelection && currentFiles[previousSelection]?.type === "file") {
      this.setSelectedFile(previousSelection);
    } else {
      const firstPath = Object.keys(currentFiles)[0];
      if (firstPath) {
        this.setSelectedFile(firstPath);
      }
    }

    // Auto-open only when files appear for the first time in this session.
    if (!hadFiles && this.#size > 0) {
      this.setShowWorkbench(true);
    }
  }

  /**
   * Clear all files from the workbench
   */
  clearFiles(): void {
    this.#files.set({});
    this.#savedFiles.clear();
    this.#selectedFile.set(undefined);
    this.#unsavedFiles.set(new Set());
    this.#size = 0;
    this.#showWorkbench.set(false);
    this.clearTerminalEvents();
  }

  /**
   * Persist current document content as the new saved baseline
   */
  saveCurrentDocument(): void {
    const filePath = this.#selectedFile.get();
    if (!filePath) return;

    const current = this.#files.get()[filePath];
    if (current?.type !== 'file') return;

    // Update saved snapshot used by reset
    this.#savedFiles.set(filePath, current.content);

    // Clear unsaved marker
    const currentUnsaved = this.#unsavedFiles.get();
    if (currentUnsaved.has(filePath)) {
      const next = new Set(currentUnsaved);
      next.delete(filePath);
      this.#unsavedFiles.set(next);
    }
  }

  /**
   * Get saved file content (original content for reset)
   */
  getSavedFileContent(filePath: string): string | undefined {
    return this.#savedFiles.get(filePath);
  }

  /**
   * Reset current document to last saved state
   */
  resetCurrentDocument(): void {
    const filePath = this.#selectedFile.get();
    if (!filePath) return;

    const savedContent = this.#savedFiles.get(filePath);
    if (savedContent === undefined) return;

    // Restore the file content
    this.#files.setKey(filePath, {
      type: 'file',
      content: savedContent,
      isBinary: false,
    });

    // Clear unsaved status
    const currentUnsaved = this.#unsavedFiles.get();
    if (currentUnsaved.has(filePath)) {
      const next = new Set(currentUnsaved);
      next.delete(filePath);
      this.#unsavedFiles.set(next);
    }
  }

  /**
   * Get current document for editor
   */
  getCurrentDocument(): EditorDocument | undefined {
    const filePath = this.#selectedFile.get();
    if (!filePath) return undefined;

    const file = this.getFile(filePath);
    if (!file) return undefined;

    return {
      value: file.content,
      isBinary: file.isBinary,
      filePath,
    };
  }

  #normalizePath(relativePath: string): string | undefined {
    const normalizedPath = relativePath.replace(/\\/g, '/').trim();
    const segments = normalizedPath.split('/').filter((segment) => {
      return segment && segment !== '.' && segment !== '..';
    });

    if (segments.length === 0) {
      return undefined;
    }

    return `${WORK_DIR}/${segments.join('/')}`;
  }
}

// Singleton instance
export const workbenchStore = new WorkbenchStore();
