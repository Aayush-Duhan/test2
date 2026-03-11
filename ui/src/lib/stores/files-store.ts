/**
 * FilesStore — manages the file map and file persistence.
 *
 * Local-only adaptation of bolt.new's FilesStore.
 * Instead of WebContainer, files are persisted via Next.js API routes
 * that hit the local filesystem.
 */

import { map, type MapStore } from 'nanostores';
import { createScopedLogger } from '@/lib/logger';

const logger = createScopedLogger('FilesStore');

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

const WORK_DIR = '/project';

export class FilesStore {
  /**
   * Tracks the number of files without folders.
   */
  #size = 0;

  /**
   * Tracks modified files with their original content since the last user message.
   * Used for computing file diffs sent to the model.
   */
  #modifiedFiles: Map<string, string> = new Map();

  /**
   * Tracks saved baselines for reset functionality.
   */
  #savedFiles: Map<string, string> = new Map();

  /**
   * Map of files that matches the current state.
   */
  files: MapStore<FileMap> = map({});

  get filesCount() {
    return this.#size;
  }

  getFile(filePath: string): File | undefined {
    const dirent = this.files.get()[filePath];

    if (dirent?.type !== 'file') {
      return undefined;
    }

    return dirent;
  }

  getFileModifications(): Map<string, string> {
    return new Map(this.#modifiedFiles);
  }

  resetFileModifications() {
    this.#modifiedFiles.clear();
  }

  /**
   * Add or update files in the store (for uploaded/imported files).
   */
  upsertVirtualFiles(fileMap: Record<string, { content: string; isBinary: boolean }>) {
    logger.info(`Upserting ${Object.keys(fileMap).length} virtual files`);

    for (const [filePath, file] of Object.entries(fileMap)) {
      const previous = this.files.get()[filePath];

      if (previous?.type !== 'file') {
        this.#size++;
      }

      this.files.setKey(filePath, {
        type: 'file',
        content: file.content,
        isBinary: file.isBinary,
      });

      // Save baseline for reset
      this.#savedFiles.set(filePath, file.content);
    }
  }

  /**
   * Persist a file to the local filesystem via API route.
   */
  async saveFile(filePath: string, content: string) {
    const currentFile = this.getFile(filePath);

    if (!currentFile) {
      logger.error(`Cannot save: file ${filePath} not found in store`);
      return;
    }

    const oldContent = currentFile.content;

    try {
      const response = await fetch('/api/fs/write', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filePath, content }),
      });

      if (!response.ok) {
        throw new Error(`Failed to write file: ${response.statusText}`);
      }

      if (!this.#modifiedFiles.has(filePath)) {
        this.#modifiedFiles.set(filePath, oldContent);
      }

      // Update the store immediately
      this.files.setKey(filePath, {
        type: 'file',
        content,
        isBinary: currentFile.isBinary,
      });

      // Update saved baseline
      this.#savedFiles.set(filePath, content);

      logger.info(`File saved: ${filePath}`);
    } catch (error) {
      logger.error('Failed to save file\n\n', error);
      throw error;
    }
  }

  /**
   * Get the saved baseline content for a file (for reset).
   */
  getSavedContent(filePath: string): string | undefined {
    return this.#savedFiles.get(filePath);
  }

  /**
   * Clear all files from the store.
   */
  clearAll() {
    this.files.set({});
    this.#modifiedFiles.clear();
    this.#savedFiles.clear();
    this.#size = 0;
  }

  /**
   * Normalize a relative file path to an absolute project path.
   */
  static normalizePath(relativePath: string): string | undefined {
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
