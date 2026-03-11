/**
 * github-store.ts
 *
 * Lightweight reactive store (same pattern as wizard-store.ts) for managing
 * the GitHub Import modal state: repo URL parsing, tree fetching, file
 * selection, and content fetching.
 */
import React from "react";

// ── Types ──────────────────────────────────────────────────────────────────────

export interface GitHubTreeEntry {
  path: string;
  sha: string;
  size: number;
}

export interface GitHubFetchedFile {
  path: string;
  content: string;
  size: number;
}

export interface GitHubImportState {
  // Inputs
  repoUrl: string;
  token: string;
  branch: string;

  // Parsed
  owner: string;
  repo: string;

  // Tree data
  tree: GitHubTreeEntry[];
  truncated: boolean;
  defaultBranch: string;

  // Selection
  selectedPaths: Set<string>;

  // Fetched file contents
  fetchedFiles: GitHubFetchedFile[];

  // Loading & errors
  isLoadingTree: boolean;
  isLoadingFiles: boolean;
  error: string | null;

  // Search / filter
  searchQuery: string;
}

// ── Initial state ──────────────────────────────────────────────────────────────

function createInitialState(): GitHubImportState {
  return {
    repoUrl: "",
    token: "",
    branch: "",
    owner: "",
    repo: "",
    tree: [],
    truncated: false,
    defaultBranch: "",
    selectedPaths: new Set(),
    fetchedFiles: [],
    isLoadingTree: false,
    isLoadingFiles: false,
    error: null,
    searchQuery: "",
  };
}

let state = createInitialState();
const listeners = new Set<() => void>();

// ── Store internals ────────────────────────────────────────────────────────────

function notify() {
  listeners.forEach((cb) => cb());
}

export function getGitHubImportState(): GitHubImportState {
  return state;
}

export function subscribeGitHubImport(callback: () => void): () => void {
  listeners.add(callback);
  return () => listeners.delete(callback);
}

// ── Setters ────────────────────────────────────────────────────────────────────

export function setRepoUrl(url: string) {
  state = { ...state, repoUrl: url, error: null };

  // Auto-parse owner/repo from URL
  const parsed = parseGitHubUrl(url);
  if (parsed) {
    state = { ...state, owner: parsed.owner, repo: parsed.repo };
  }
  notify();
}

export function setToken(token: string) {
  state = { ...state, token, error: null };
  notify();
}

export function setBranch(branch: string) {
  state = { ...state, branch, error: null };
  notify();
}

export function setSearchQuery(query: string) {
  state = { ...state, searchQuery: query };
  notify();
}

// ── Selection actions ──────────────────────────────────────────────────────────

export function toggleFile(path: string) {
  const next = new Set(state.selectedPaths);
  if (next.has(path)) {
    next.delete(path);
  } else {
    next.add(path);
  }
  state = { ...state, selectedPaths: next };
  notify();
}

export function selectAllVisible(paths: string[]) {
  const next = new Set(state.selectedPaths);
  for (const p of paths) next.add(p);
  state = { ...state, selectedPaths: next };
  notify();
}

export function deselectAllVisible(paths: string[]) {
  const next = new Set(state.selectedPaths);
  for (const p of paths) next.delete(p);
  state = { ...state, selectedPaths: next };
  notify();
}

export function clearSelection() {
  state = { ...state, selectedPaths: new Set() };
  notify();
}

// ── Async actions ──────────────────────────────────────────────────────────────

export async function fetchTree() {
  const { owner, repo, branch, token } = state;
  if (!owner || !repo) {
    state = { ...state, error: "Please enter a valid GitHub repository URL." };
    notify();
    return;
  }

  state = {
    ...state,
    isLoadingTree: true,
    error: null,
    tree: [],
    selectedPaths: new Set(),
    fetchedFiles: [],
  };
  notify();

  try {
    const res = await fetch("/api/github/tree", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        owner,
        repo,
        branch: branch || undefined,
        token: token || undefined,
      }),
    });

    const data = await res.json();
    if (!res.ok) {
      state = {
        ...state,
        isLoadingTree: false,
        error: data.error ?? "Failed to fetch repository tree.",
      };
      notify();
      return;
    }

    state = {
      ...state,
      isLoadingTree: false,
      tree: data.tree ?? [],
      truncated: data.truncated ?? false,
      defaultBranch: data.defaultBranch ?? "",
    };
    notify();
  } catch {
    state = {
      ...state,
      isLoadingTree: false,
      error: "Network error while fetching repository tree.",
    };
    notify();
  }
}

export async function fetchSelectedFiles(): Promise<GitHubFetchedFile[]> {
  const { owner, repo, token, selectedPaths, tree } = state;
  if (selectedPaths.size === 0) return [];

  const filesToFetch = tree
    .filter((e) => selectedPaths.has(e.path))
    .map((e) => ({ path: e.path, sha: e.sha }));

  state = { ...state, isLoadingFiles: true, error: null };
  notify();

  try {
    const res = await fetch("/api/github/files", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        owner,
        repo,
        files: filesToFetch,
        token: token || undefined,
      }),
    });

    const data = await res.json();
    if (!res.ok) {
      state = {
        ...state,
        isLoadingFiles: false,
        error: data.error ?? "Failed to fetch file contents.",
      };
      notify();
      return [];
    }

    const fetched: GitHubFetchedFile[] = data.files ?? [];
    state = { ...state, isLoadingFiles: false, fetchedFiles: fetched };
    notify();
    return fetched;
  } catch {
    state = {
      ...state,
      isLoadingFiles: false,
      error: "Network error while fetching file contents.",
    };
    notify();
    return [];
  }
}

// ── Reset ──────────────────────────────────────────────────────────────────────

export function resetGitHubImport() {
  state = createInitialState();
  notify();
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function parseGitHubUrl(
  url: string
): { owner: string; repo: string } | null {
  if (!url) return null;

  // Match patterns:
  //   https://github.com/owner/repo
  //   https://github.com/owner/repo.git
  //   github.com/owner/repo
  //   owner/repo
  const patterns = [
    /(?:https?:\/\/)?github\.com\/([^/\s]+)\/([^/\s.]+?)(?:\.git)?(?:\/|$)/i,
    /^([^/\s]+)\/([^/\s]+)$/,
  ];

  for (const pattern of patterns) {
    const match = url.trim().match(pattern);
    if (match) {
      return { owner: match[1], repo: match[2] };
    }
  }
  return null;
}

/**
 * Filter the tree by the current search query and optionally by file extensions.
 */
export function getFilteredTree(extensionFilter?: string[]): GitHubTreeEntry[] {
  let filtered = state.tree;

  if (extensionFilter && extensionFilter.length > 0) {
    filtered = filtered.filter((entry) => {
      const ext = entry.path.split(".").pop()?.toLowerCase() ?? "";
      return extensionFilter.includes(ext);
    });
  }

  if (state.searchQuery.trim()) {
    const q = state.searchQuery.toLowerCase();
    filtered = filtered.filter((entry) =>
      entry.path.toLowerCase().includes(q)
    );
  }

  return filtered;
}

// ── React hook ─────────────────────────────────────────────────────────────────

export function useGitHubImportState(): GitHubImportState {
  const [current, setCurrent] = React.useState<GitHubImportState>(getGitHubImportState);

  React.useEffect(() => {
    return subscribeGitHubImport(() => {
      setCurrent(getGitHubImportState());
    });
  }, []);

  return current;
}
