/**
 * Project File Watcher — watches source & output directories for changes
 * and notifies subscribed SSE clients so they can refresh the workbench.
 *
 * Watchers are ref-counted: created on first subscriber, closed when last
 * subscriber disconnects.
 */

import * as path from 'path';
import chokidar, { type FSWatcher } from 'chokidar';

export interface FileChangeEvent {
    /** Virtual path relative to project root (e.g. "output/foo.sql") */
    path: string;
    /** Type of change */
    type: 'add' | 'change' | 'unlink';
}

type Subscriber = (event: FileChangeEvent) => void;

interface WatcherEntry {
    watcher: FSWatcher;
    subscribers: Set<Subscriber>;
}

const PROJECTS_DIR = path.resolve(process.cwd(), '..', 'projects');

/**
 * Real-on-disk roots we care about, mapped to the virtual prefix the UI uses.
 * Must stay in sync with VIRTUAL_ROOTS in the files API route.
 */
const WATCH_ROOTS: Array<{ diskRelative: string; virtualPrefix: string }> = [
    { diskRelative: 'source', virtualPrefix: 'source' },
    { diskRelative: path.join('converted', 'Output', 'SnowConvert'), virtualPrefix: 'output' },
    { diskRelative: path.join('converted', 'Reports'), virtualPrefix: 'reports' },
];

const watchers = new Map<string, WatcherEntry>();

function toPosix(p: string) {
    return p.split(path.sep).join('/');
}

/**
 * Map a real absolute path back to a virtual path like "output/foo.sql".
 */
function toVirtualPath(absPath: string, projectRoot: string): string | null {
    const rel = toPosix(path.relative(projectRoot, absPath));
    for (const { diskRelative, virtualPrefix } of WATCH_ROOTS) {
        const disk = toPosix(diskRelative);
        if (rel === disk) return virtualPrefix;
        if (rel.startsWith(disk + '/')) {
            return virtualPrefix + rel.slice(disk.length);
        }
    }
    return null;
}

function createWatcher(projectName: string): WatcherEntry {
    const projectRoot = path.join(PROJECTS_DIR, projectName);

    // Watch the entire project root so we detect files even when parent
    // directories (e.g. converted/Output/SnowConvert) are created after
    // the watcher starts.  The `toVirtualPath` filter in the notify
    // callback ensures only files under allowed roots trigger events.
    const watcher = chokidar.watch(projectRoot, {
        ignoreInitial: true,
        persistent: true,
        // Small stabilisation delay so rapid multi-file writes are batched
        awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
    });

    const entry: WatcherEntry = { watcher, subscribers: new Set() };

    const notify = (type: FileChangeEvent['type'], absPath: string) => {
        const virtualPath = toVirtualPath(absPath, projectRoot);
        if (!virtualPath) return;
        const event: FileChangeEvent = { path: virtualPath, type };
        for (const sub of entry.subscribers) {
            try { sub(event); } catch { /* subscriber error – ignore */ }
        }
    };

    watcher.on('add', (p) => notify('add', p));
    watcher.on('change', (p) => notify('change', p));
    watcher.on('unlink', (p) => notify('unlink', p));

    return entry;
}

/**
 * Subscribe to file changes for a project. Returns an unsubscribe function.
 */
export function subscribeToProject(projectName: string, callback: Subscriber): () => void {
    let entry = watchers.get(projectName);
    if (!entry) {
        entry = createWatcher(projectName);
        watchers.set(projectName, entry);
    }
    entry.subscribers.add(callback);

    return () => {
        entry!.subscribers.delete(callback);
        // Close watcher when last subscriber leaves
        if (entry!.subscribers.size === 0) {
            void entry!.watcher.close();
            watchers.delete(projectName);
        }
    };
}
