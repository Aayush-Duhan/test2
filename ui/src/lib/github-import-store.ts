import React from "react";

export interface RepoTreeEntry {
  path: string;
  sha: string;
  size: number;
}

export interface RepoFetchedFile {
  path: string;
  content: string;
  size: number;
}

export interface GitHubBranchOption {
  name: string;
  isDefault: boolean;
}

export interface GitHubImportState {
  token: string;
  org: string;
  selectedRepositoryName: string;
  isConnected: boolean;
  availableBranches: GitHubBranchOption[];
  branch: string;
  tree: RepoTreeEntry[];
  truncated: boolean;
  defaultBranch: string;
  selectedPaths: Set<string>;
  fetchedFiles: RepoFetchedFile[];
  isValidatingToken: boolean;
  isLoadingBranches: boolean;
  isLoadingTree: boolean;
  isLoadingFiles: boolean;
  error: string | null;
  warning: string | null;
  ssoUrl: string | null;
  searchQuery: string;
}

interface FetchFilesResult {
  files: RepoFetchedFile[];
  errors?: string[];
}

const STORAGE_KEY = "gh_enterprise_pat";
const ORG_STORAGE_KEY = "gh_enterprise_org";

function loadPersistedToken(): string {
  try {
    return sessionStorage.getItem(STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

function loadPersistedOrg(): string {
  try {
    return sessionStorage.getItem(ORG_STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

function persistToken(token: string) {
  try {
    if (token) {
      sessionStorage.setItem(STORAGE_KEY, token);
    } else {
      sessionStorage.removeItem(STORAGE_KEY);
    }
  } catch {
    // SSR or storage unavailable
  }
}

function persistOrg(org: string) {
  try {
    if (org) {
      sessionStorage.setItem(ORG_STORAGE_KEY, org);
    } else {
      sessionStorage.removeItem(ORG_STORAGE_KEY);
    }
  } catch {
    // SSR or storage unavailable
  }
}

function createInitialState(): GitHubImportState {
  return {
    token: "",
    org: "",
    selectedRepositoryName: "",
    isConnected: false,
    availableBranches: [],
    branch: "",
    tree: [],
    truncated: false,
    defaultBranch: "",
    selectedPaths: new Set(),
    fetchedFiles: [],
    isValidatingToken: false,
    isLoadingBranches: false,
    isLoadingTree: false,
    isLoadingFiles: false,
    error: null,
    warning: null,
    ssoUrl: null,
    searchQuery: "",
  };
}

let state = createInitialState();
const listeners = new Set<() => void>();

function notify() {
  listeners.forEach((callback) => callback());
}

function handleSsoError(data: Record<string, unknown> | null): boolean {
  const ssoUrl = typeof data?.ssoUrl === "string" ? data.ssoUrl : null;
  if (ssoUrl) {
    state = {
      ...state,
      error: typeof data?.error === "string" ? data.error : "SSO authorization required.",
      ssoUrl,
    };
    notify();
    return true;
  }
  return false;
}

export function getGitHubImportState(): GitHubImportState {
  return state;
}

export function subscribeGitHubImport(callback: () => void): () => void {
  listeners.add(callback);
  return () => listeners.delete(callback);
}

export function setToken(token: string) {
  state = {
    ...state,
    token,
    error: null,
    ssoUrl: null,
    isConnected: false,
    availableBranches: [],
    branch: "",
    defaultBranch: "",
    tree: [],
    truncated: false,
    selectedPaths: new Set(),
    fetchedFiles: [],
  };
  persistToken(token);
  notify();
}

export function setOrg(org: string) {
  state = {
    ...state,
    org,
    error: null,
    ssoUrl: null,
    isConnected: false,
    availableBranches: [],
    branch: "",
    defaultBranch: "",
    tree: [],
    truncated: false,
    selectedPaths: new Set(),
    fetchedFiles: [],
  };
  persistOrg(org);
  notify();
}

export function setRepositoryName(repositoryName: string) {
  state = {
    ...state,
    selectedRepositoryName: repositoryName,
    isConnected: false,
    availableBranches: [],
    branch: "",
    defaultBranch: "",
    tree: [],
    truncated: false,
    selectedPaths: new Set(),
    fetchedFiles: [],
    error: null,
    warning: null,
    ssoUrl: null,
  };
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

export function dismissWarning() {
  state = { ...state, warning: null };
  notify();
}

export function dismissSso() {
  state = { ...state, ssoUrl: null };
  notify();
}

export const dismissSsoNotice = dismissSso;

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
  for (const path of paths) next.add(path);
  state = { ...state, selectedPaths: next };
  notify();
}

export function deselectAllVisible(paths: string[]) {
  const next = new Set(state.selectedPaths);
  for (const path of paths) next.delete(path);
  state = { ...state, selectedPaths: next };
  notify();
}

export function clearStoredCredentials() {
  persistToken("");
  persistOrg("");
  state = createInitialState();
  notify();
}

export function clearGitHubSelection(options?: {
  resetSearch?: boolean;
  clearWarning?: boolean;
  clearError?: boolean;
}) {
  const resetSearch = options?.resetSearch ?? true;
  const clearWarning = options?.clearWarning ?? true;
  const clearError = options?.clearError ?? true;
  state = {
    ...state,
    selectedPaths: new Set(),
    fetchedFiles: [],
    ...(clearWarning ? { warning: null } : {}),
    ...(clearError ? { error: null } : {}),
    ...(resetSearch ? { searchQuery: "" } : {}),
  };
  notify();
}

async function fetchBranchesForRepository(repositoryName: string) {
  const trimmedRepository = repositoryName.trim();
  const token = state.token.trim();
  const org = state.org.trim();
  if (!token || !org || !trimmedRepository) return;

  state = {
    ...state,
    isLoadingBranches: true,
    isConnected: false,
    availableBranches: [],
    branch: "",
    defaultBranch: "",
    warning: null,
    error: null,
    ssoUrl: null,
  };
  notify();

  try {
    const response = await fetch("/api/github/branches", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        org,
        repositoryName: trimmedRepository,
      }),
    });

    const data = await response.json().catch(() => null);
    if (!response.ok) {
      if (handleSsoError(data)) {
        state = { ...state, isLoadingBranches: false, isConnected: false };
        notify();
        return;
      }
      state = {
        ...state,
        isLoadingBranches: false,
        isConnected: false,
        availableBranches: [],
        branch: "",
        defaultBranch: "",
        warning: "Branch list unavailable. The default branch will be used.",
      };
      notify();
      return;
    }

    const availableBranches = Array.isArray(data?.branches)
      ? (data.branches as GitHubBranchOption[])
      : [];
    const defaultBranch =
      typeof data?.defaultBranch === "string" ? data.defaultBranch : "";

    state = {
      ...state,
      isLoadingBranches: false,
      isConnected: true,
      availableBranches,
      defaultBranch,
      branch: defaultBranch || availableBranches[0]?.name || "",
      warning: null,
    };
    notify();
  } catch {
    state = {
      ...state,
      isLoadingBranches: false,
      isConnected: false,
      availableBranches: [],
      branch: "",
      defaultBranch: "",
      warning: "Branch list unavailable. The default branch will be used.",
    };
    notify();
  }
}

export async function connectRepository() {
  const token = state.token.trim();
  const org = state.org.trim();
  const repositoryName = state.selectedRepositoryName.trim();

  if (!token) {
    state = { ...state, error: "Please enter a GitHub Personal Access Token." };
    notify();
    return;
  }
  if (!org) {
    state = { ...state, error: "Please enter a GitHub organization name." };
    notify();
    return;
  }
  if (!repositoryName) {
    state = { ...state, error: "Please enter a GitHub repository name." };
    notify();
    return;
  }

  state = {
    ...state,
    error: null,
    warning: null,
    ssoUrl: null,
  };
  notify();

  await fetchBranchesForRepository(repositoryName);
}

export async function fetchTree() {
  const repositoryName = state.selectedRepositoryName.trim();
  const token = state.token.trim();
  const org = state.org.trim();
  if (!token || !org || !repositoryName) {
    state = {
      ...state,
      error: "Please enter a GitHub token, organization, and repository name.",
    };
    notify();
    return;
  }

  state = {
    ...state,
    isLoadingTree: true,
    error: null,
    ssoUrl: null,
    tree: [],
    truncated: false,
    selectedPaths: new Set(),
    fetchedFiles: [],
  };
  notify();

  try {
    const response = await fetch("/api/github/tree", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        org,
        repositoryName,
        branch: state.branch || undefined,
      }),
    });

    const data = await response.json().catch(() => null);
    if (!response.ok) {
      if (handleSsoError(data)) {
        state = { ...state, isLoadingTree: false, isConnected: false };
        notify();
        return;
      }
      state = {
        ...state,
        isLoadingTree: false,
        isConnected: false,
        error: data?.error ?? "Failed to load repository tree.",
      };
      notify();
      return;
    }

    const defaultBranch =
      typeof data?.defaultBranch === "string" ? data.defaultBranch : state.defaultBranch;

    state = {
      ...state,
      isLoadingTree: false,
      isConnected: true,
      tree: Array.isArray(data?.tree) ? (data.tree as RepoTreeEntry[]) : [],
      truncated: data?.truncated === true,
      defaultBranch,
      branch: state.branch || defaultBranch,
    };
    notify();
  } catch {
    state = {
      ...state,
      isLoadingTree: false,
      isConnected: false,
      error: "Network error while loading the repository tree.",
    };
    notify();
  }
}

/**
 * Single-action: fetch branches (connect) then immediately fetch the tree.
 * This is the only entry point the modal needs.
 */
export async function loadRepository() {
  await connectRepository();

  // If connecting failed (error set, not connected), bail out.
  if (!state.isConnected) return;

  await fetchTree();
}

/**
 * Change the selected branch and re-fetch the tree in one action.
 */
export async function changeBranchAndReload(newBranch: string) {
  state = { ...state, branch: newBranch, error: null };
  notify();
  await fetchTree();
}

export async function fetchSelectedFiles(): Promise<FetchFilesResult> {
  const repositoryName = state.selectedRepositoryName.trim();
  const token = state.token.trim();
  const org = state.org.trim();
  if (!token || !org || !repositoryName || state.selectedPaths.size === 0) {
    return { files: [] };
  }

  const filesToFetch = state.tree
    .filter((entry) => state.selectedPaths.has(entry.path))
    .map((entry) => ({ path: entry.path, sha: entry.sha }));

  state = {
    ...state,
    isLoadingFiles: true,
    error: null,
    warning: null,
    ssoUrl: null,
  };
  notify();

  try {
    const response = await fetch("/api/github/files", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        org,
        repositoryName,
        branch: state.branch || state.defaultBranch || undefined,
        files: filesToFetch,
      }),
    });

    const data = await response.json().catch(() => null);
    if (!response.ok) {
      if (handleSsoError(data)) {
        state = { ...state, isLoadingFiles: false };
        notify();
        return { files: [] };
      }
      state = {
        ...state,
        isLoadingFiles: false,
        error: data?.error ?? "Failed to load file contents.",
      };
      notify();
      return { files: [] };
    }

    const fetchedFiles = Array.isArray(data?.files)
      ? (data.files as RepoFetchedFile[])
      : [];
    const errors = Array.isArray(data?.errors)
      ? data.errors.filter((item: unknown): item is string => typeof item === "string")
      : undefined;

    state = {
      ...state,
      isLoadingFiles: false,
      fetchedFiles,
      warning:
        errors && errors.length > 0
          ? `Imported ${fetchedFiles.length} file(s). Some files could not be loaded.`
          : null,
    };
    notify();

    return { files: fetchedFiles, errors };
  } catch {
    state = {
      ...state,
      isLoadingFiles: false,
      error: "Network error while loading file contents.",
    };
    notify();
    return { files: [] };
  }
}

export function resetGitHubImport() {
  const token = loadPersistedToken();
  const org = loadPersistedOrg();
  state = { ...createInitialState(), token, org };
  notify();
}

export function getFilteredTree(extensionFilter?: string[]): RepoTreeEntry[] {
  let filtered = state.tree;

  if (extensionFilter && extensionFilter.length > 0) {
    filtered = filtered.filter((entry) => {
      const extension = entry.path.split(".").pop()?.toLowerCase() ?? "";
      return extensionFilter.includes(extension);
    });
  }

  if (state.searchQuery.trim()) {
    const query = state.searchQuery.toLowerCase();
    filtered = filtered.filter((entry) =>
      entry.path.toLowerCase().includes(query)
    );
  }

  return filtered;
}

export function useGitHubImportState(): GitHubImportState {
  const [current, setCurrent] = React.useState<GitHubImportState>(getGitHubImportState);

  React.useEffect(() => {
    return subscribeGitHubImport(() => {
      setCurrent(getGitHubImportState());
    });
  }, []);

  return current;
}
