/**
 * WorkbenchStore — composes FilesStore + EditorStore + TerminalStore.
 *
 * Full port of bolt.new's WorkbenchStore, adapted for local-only execution.
 * No WebContainer dependency — all file ops go through the local FS API routes
 * and the terminal uses the existing WebSocket connection.
 */

import { atom, map, type MapStore, type ReadableAtom, type WritableAtom } from 'nanostores';
import { FilesStore, type FileMap } from '@/lib/stores/files-store';
import { EditorStore, type EditorDocument, type ScrollPosition } from '@/lib/stores/editor-store';
import { terminalStore } from '@/lib/terminal-store';
import { ActionRunner } from '@/lib/runtime/action-runner';
import type { ActionCallbackData, ArtifactCallbackData } from '@/lib/runtime/message-parser';
import type { BoltArtifactData } from '@/lib/types/artifact';
import { createScopedLogger } from '@/lib/logger';
import { unreachable } from '@/lib/utils/unreachable';

const logger = createScopedLogger('WorkbenchStore');

export interface ArtifactState {
  id: string;
  title: string;
  closed: boolean;
  runner: ActionRunner;
}

export type ArtifactUpdateState = Pick<ArtifactState, 'title' | 'closed'>;

type Artifacts = MapStore<Record<string, ArtifactState>>;

export interface UploadedFile {
  name: string;
  content: string;
  relativePath?: string;
  isBinary?: boolean;
}

export class WorkbenchStore {
  #filesStore = new FilesStore();
  #editorStore = new EditorStore(this.#filesStore);

  artifacts: Artifacts = map({});

  showWorkbench: WritableAtom<boolean> = atom(false);
  unsavedFiles: WritableAtom<Set<string>> = atom(new Set<string>());
  modifiedFiles = new Set<string>();
  artifactIdList: string[] = [];

  get files(): MapStore<FileMap> {
    return this.#filesStore.files;
  }

  get currentDocument(): ReadableAtom<EditorDocument | undefined> {
    return this.#editorStore.currentDocument;
  }

  get selectedFile(): ReadableAtom<string | undefined> {
    return this.#editorStore.selectedFile;
  }

  get firstArtifact(): ArtifactState | undefined {
    return this.#getArtifact(this.artifactIdList[0]);
  }

  get filesCount(): number {
    return this.#filesStore.filesCount;
  }

  get showTerminal() {
    return terminalStore.showTerminal;
  }

  toggleTerminal(value?: boolean) {
    terminalStore.toggleTerminal(value);
  }

  setDocuments(files: FileMap) {
    this.#editorStore.setDocuments(files);

    if (this.#filesStore.filesCount > 0 && this.currentDocument.get() === undefined) {
      // find the first file and select it
      for (const [filePath, dirent] of Object.entries(files)) {
        if (dirent?.type === 'file') {
          this.setSelectedFile(filePath);
          break;
        }
      }
    }
  }

  setShowWorkbench(show: boolean) {
    this.showWorkbench.set(show);
  }

  addUploadedFiles(files: UploadedFile[]) {
    logger.info(`addUploadedFiles called with ${files.length} files`);

    const fileMap: Record<string, { content: string; isBinary: boolean }> = {};

    for (const file of files) {
      const normalizedPath = FilesStore.normalizePath(file.relativePath ?? file.name);

      if (!normalizedPath) {
        continue;
      }

      fileMap[normalizedPath] = {
        content: file.content,
        isBinary: file.isBinary ?? false,
      };
    }

    logger.info(`Mapped ${Object.keys(fileMap).length} uploaded files into workbench paths`);
    this.#filesStore.upsertVirtualFiles(fileMap);

    const firstUploadedPath = Object.keys(fileMap)[0];

    if (firstUploadedPath) {
      logger.info(`Selecting first uploaded file: ${firstUploadedPath}`);
      this.setSelectedFile(firstUploadedPath);
    }

    logger.info('Showing workbench after upload import');
    this.setShowWorkbench(true);
  }

  setCurrentDocumentContent(newContent: string) {
    const filePath = this.currentDocument.get()?.filePath;

    if (!filePath) {
      return;
    }

    const originalContent = this.#filesStore.getFile(filePath)?.content;
    const unsavedChanges = originalContent !== undefined && originalContent !== newContent;

    this.#editorStore.updateFile(filePath, newContent);

    const currentDocument = this.currentDocument.get();

    if (currentDocument) {
      const previousUnsavedFiles = this.unsavedFiles.get();

      if (unsavedChanges && previousUnsavedFiles.has(currentDocument.filePath)) {
        return;
      }

      const newUnsavedFiles = new Set(previousUnsavedFiles);

      if (unsavedChanges) {
        newUnsavedFiles.add(currentDocument.filePath);
      } else {
        newUnsavedFiles.delete(currentDocument.filePath);
      }

      this.unsavedFiles.set(newUnsavedFiles);
    }
  }

  setCurrentDocumentScrollPosition(position: ScrollPosition) {
    const editorDocument = this.currentDocument.get();

    if (!editorDocument) {
      return;
    }

    const { filePath } = editorDocument;

    this.#editorStore.updateScrollPosition(filePath, position);
  }

  setSelectedFile(filePath: string | undefined) {
    this.#editorStore.setSelectedFile(filePath);
  }

  async saveFile(filePath: string) {
    const documents = this.#editorStore.documents.get();
    const document = documents[filePath];

    if (document === undefined) {
      return;
    }

    await this.#filesStore.saveFile(filePath, document.value);

    const newUnsavedFiles = new Set(this.unsavedFiles.get());
    newUnsavedFiles.delete(filePath);

    this.unsavedFiles.set(newUnsavedFiles);
  }

  async saveCurrentDocument() {
    const currentDocument = this.currentDocument.get();

    if (currentDocument === undefined) {
      return;
    }

    await this.saveFile(currentDocument.filePath);
  }

  resetCurrentDocument() {
    const currentDocument = this.currentDocument.get();

    if (currentDocument === undefined) {
      return;
    }

    const { filePath } = currentDocument;
    const file = this.#filesStore.getFile(filePath);

    if (!file) {
      return;
    }

    this.setCurrentDocumentContent(file.content);
  }

  async saveAllFiles() {
    for (const filePath of this.unsavedFiles.get()) {
      await this.saveFile(filePath);
    }
  }

  getFileModifications() {
    return this.#filesStore.getFileModifications();
  }

  resetAllFileModifications() {
    this.#filesStore.resetFileModifications();
  }

  clearFiles() {
    this.#filesStore.clearAll();
    this.unsavedFiles.set(new Set());
    this.showWorkbench.set(false);
  }

  abortAllActions() {
    // TODO: what do we wanna do and how do we wanna recover from this?
  }

  addArtifact({ messageId, title, id }: ArtifactCallbackData) {
    const artifact = this.#getArtifact(messageId);

    if (artifact) {
      return;
    }

    if (!this.artifactIdList.includes(messageId)) {
      this.artifactIdList.push(messageId);
    }

    this.artifacts.setKey(messageId, {
      id,
      title,
      closed: false,
      runner: new ActionRunner(),
    });
  }

  updateArtifact({ messageId }: ArtifactCallbackData, state: Partial<ArtifactUpdateState>) {
    const artifact = this.#getArtifact(messageId);

    if (!artifact) {
      return;
    }

    this.artifacts.setKey(messageId, { ...artifact, ...state });
  }

  async addAction(data: ActionCallbackData) {
    const { messageId } = data;

    const artifact = this.#getArtifact(messageId);

    if (!artifact) {
      unreachable('Artifact not found');
    }

    artifact.runner.addAction(data);
  }

  async runAction(data: ActionCallbackData) {
    const { messageId } = data;

    const artifact = this.#getArtifact(messageId);

    if (!artifact) {
      unreachable('Artifact not found');
    }

    artifact.runner.runAction(data);
  }

  #getArtifact(id: string) {
    const artifacts = this.artifacts.get();
    return artifacts[id];
  }

  // === Backward-compatible stubs ===
  // These methods were used by the old structured terminal command log.
  // With the new real xterm terminal, they are no-ops but kept for
  // compatibility with sessions/page.tsx hydration code.

  /** @deprecated Terminal is now a real xterm instance. This is a no-op. */
  clearTerminal(): void {
    // no-op: real terminal is managed by terminalStore via WebSocket
  }

  /** @deprecated Terminal is now a real xterm instance. This is a no-op. */
  replaceTerminalCommands(_commands: TerminalCommand[]): void {
    // no-op: real terminal is managed by terminalStore via WebSocket
  }
}

// Legacy type kept for backward compatibility with sessions/page.tsx
export interface TerminalLine {
  text: string;
  isProgress: boolean;
  ts: number;
}

export interface TerminalCommand {
  id: string;
  label: string;
  stepId?: string;
  lines: TerminalLine[];
  isComplete: boolean;
  ts: number;
}

// Re-export types for convenience
export type { EditorDocument, ScrollPosition, FileMap, UploadedFile as UploadedWorkbenchFile };

// Singleton instance
export const workbenchStore = new WorkbenchStore();

