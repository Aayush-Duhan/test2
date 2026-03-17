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

export interface GitHubRepositoryOption {
  id?: number | null;
  name: string;
  visibility?: string | null;
}

export interface GitHubBranchOption {
  name: string;
  isDefault: boolean;
}

export interface GitHubImportState {
  token: string;
  org: string;
  repositories: GitHubRepositoryOption[];
  selectedRepositoryName: string;
  availableBranches: GitHubBranchOption[];
  branch: string;
  tree: RepoTreeEntry[];
  truncated: boolean;
  defaultBranch: string;
  selectedPaths: Set<string>;
  fetchedFiles: RepoFetchedFile[];
  isValidatingToken: boolean;
  isLoadingRepos: boolean;
  isLoadingBranches: boolean;
  isLoadingTree: boolean;
  isLoadingFiles: boolean;
  error: string | null;
  warning: string | null;
  ssoUrl: string | null;
  searchQuery: string;
  hasMore: boolean;
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
    repositories: [],
    selectedRepositoryName: "",
    availableBranches: [],
    branch: "",
    tree: [],
    truncated: false,
    defaultBranch: "",
    selectedPaths: new Set(),
    fetchedFiles: [],
    isValidatingToken: false,
    isLoadingRepos: false,
    isLoadingBranches: false,
    isLoadingTree: false,
    isLoadingFiles: false,
    error: null,
    warning: null,
    ssoUrl: null,
    searchQuery: "",
    hasMore: false,
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
  };
  persistOrg(org);
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

async function fetchBranchesForRepository(repositoryName: string) {
  if (!state.token || !state.org || !repositoryName) return;

  state = {
    ...state,
    isLoadingBranches: true,
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
        token: state.token,
        org: state.org,
        repositoryName,
      }),
    });

    const data = await response.json().catch(() => null);
    if (!response.ok) {
      if (handleSsoError(data)) {
        state = { ...state, isLoadingBranches: false };
        notify();
        return;
      }
      state = {
        ...state,
        isLoadingBranches: false,
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
      availableBranches: [],
      branch: "",
      defaultBranch: "",
      warning: "Branch list unavailable. The default branch will be used.",
    };
    notify();
  }
}

export async function fetchRepositories() {
  const token = state.token.trim();
  const org = state.org.trim();

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

  state = {
    ...state,
    isLoadingRepos: true,
    error: null,
    warning: null,
    ssoUrl: null,
    repositories: [],
    selectedRepositoryName: "",
    availableBranches: [],
    branch: "",
    defaultBranch: "",
    tree: [],
    truncated: false,
    selectedPaths: new Set(),
    fetchedFiles: [],
  };
  notify();

  try {
    const response = await fetch("/api/github/repos", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token, org }),
    });

    const data = await response.json().catch(() => null);
    if (!response.ok) {
      if (handleSsoError(data)) {
        state = { ...state, isLoadingRepos: false };
        notify();
        return;
      }
      state = {
        ...state,
        isLoadingRepos: false,
        error: data?.error ?? "Failed to load repositories.",
      };
      notify();
      return;
    }

    const repositories = Array.isArray(data?.repositories)
      ? (data.repositories as GitHubRepositoryOption[])
      : [];
    const selectedRepositoryName =
      repositories.length === 1 ? repositories[0].name : "";

    state = {
      ...state,
      isLoadingRepos: false,
      repositories,
      selectedRepositoryName,
      hasMore: data?.hasMore === true,
      warning:
        repositories.length === 0
          ? "This organization does not have any accessible repositories."
          : null,
    };
    notify();

    if (selectedRepositoryName) {
      await fetchBranchesForRepository(selectedRepositoryName);
    }
  } catch {
    state = {
      ...state,
      isLoadingRepos: false,
      error: "Network error while loading repositories.",
    };
    notify();
  }
}

export async function selectRepository(repositoryName: string) {
  const nextRepository =
    state.repositories.find((r) => r.name === repositoryName) ?? null;

  state = {
    ...state,
    selectedRepositoryName: nextRepository?.name ?? "",
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

  if (nextRepository) {
    await fetchBranchesForRepository(nextRepository.name);
  }
}

export async function fetchTree() {
  if (!state.token || !state.org || !state.selectedRepositoryName) {
    state = {
      ...state,
      error: "Please connect and select a repository.",
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
        token: state.token,
        org: state.org,
        repositoryName: state.selectedRepositoryName,
        branch: state.branch || undefined,
      }),
    });

    const data = await response.json().catch(() => null);
    if (!response.ok) {
      if (handleSsoError(data)) {
        state = { ...state, isLoadingTree: false };
        notify();
        return;
      }
      state = {
        ...state,
        isLoadingTree: false,
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
      error: "Network error while loading the repository tree.",
    };
    notify();
  }
}

export async function fetchSelectedFiles(): Promise<FetchFilesResult> {
  if (!state.token || !state.org || !state.selectedRepositoryName || state.selectedPaths.size === 0) {
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
        token: state.token,
        org: state.org,
        repositoryName: state.selectedRepositoryName,
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
