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

export interface CodeHubRepositoryOption {
  id?: number | string | null;
  name: string;
  visibility?: string | null;
  technology?: string | null;
  template?: string | null;
  topics?: string[];
}

export interface CodeHubBranchOption {
  name: string;
  isDefault: boolean;
}

export interface CodeHubImportState {
  offeringName: string;
  offeringId: string;
  teamName: string;
  repositories: CodeHubRepositoryOption[];
  selectedRepositoryName: string;
  availableBranches: CodeHubBranchOption[];
  branch: string;
  tree: RepoTreeEntry[];
  truncated: boolean;
  defaultBranch: string;
  selectedPaths: Set<string>;
  fetchedFiles: RepoFetchedFile[];
  isLoadingOffering: boolean;
  isLoadingBranches: boolean;
  isLoadingTree: boolean;
  isLoadingFiles: boolean;
  error: string | null;
  warning: string | null;
  searchQuery: string;
}

interface FetchFilesResult {
  files: RepoFetchedFile[];
  errors?: string[];
}

function createInitialState(): CodeHubImportState {
  return {
    offeringName: "",
    offeringId: "",
    teamName: "",
    repositories: [],
    selectedRepositoryName: "",
    availableBranches: [],
    branch: "",
    tree: [],
    truncated: false,
    defaultBranch: "",
    selectedPaths: new Set(),
    fetchedFiles: [],
    isLoadingOffering: false,
    isLoadingBranches: false,
    isLoadingTree: false,
    isLoadingFiles: false,
    error: null,
    warning: null,
    searchQuery: "",
  };
}

let state = createInitialState();
const listeners = new Set<() => void>();

function notify() {
  listeners.forEach((callback) => callback());
}

function getSelectedRepository(): CodeHubRepositoryOption | null {
  return (
    state.repositories.find((repository) => repository.name === state.selectedRepositoryName) ??
    null
  );
}

export function getCodeHubImportState(): CodeHubImportState {
  return state;
}

export function subscribeCodeHubImport(callback: () => void): () => void {
  listeners.add(callback);
  return () => listeners.delete(callback);
}

export function setOfferingName(offeringName: string) {
  state = {
    ...state,
    offeringName,
    error: null,
    warning: null,
  };
  notify();
}

export function setBranch(branch: string) {
  state = {
    ...state,
    branch,
    error: null,
  };
  notify();
}

export function setSearchQuery(query: string) {
  state = {
    ...state,
    searchQuery: query,
  };
  notify();
}

export function dismissWarning() {
  state = {
    ...state,
    warning: null,
  };
  notify();
}

export function toggleFile(path: string) {
  const next = new Set(state.selectedPaths);
  if (next.has(path)) {
    next.delete(path);
  } else {
    next.add(path);
  }

  state = {
    ...state,
    selectedPaths: next,
  };
  notify();
}

export function selectAllVisible(paths: string[]) {
  const next = new Set(state.selectedPaths);
  for (const path of paths) next.add(path);

  state = {
    ...state,
    selectedPaths: next,
  };
  notify();
}

export function deselectAllVisible(paths: string[]) {
  const next = new Set(state.selectedPaths);
  for (const path of paths) next.delete(path);

  state = {
    ...state,
    selectedPaths: next,
  };
  notify();
}

async function fetchBranchesForRepository(repositoryName: string) {
  const repository =
    state.repositories.find((entry) => entry.name === repositoryName) ?? null;

  if (!state.offeringId || !repository) return;

  state = {
    ...state,
    isLoadingBranches: true,
    availableBranches: [],
    branch: "",
    defaultBranch: "",
    warning: null,
    error: null,
  };
  notify();

  try {
    const response = await fetch("/api/codehub/branches", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        offeringId: state.offeringId,
        repositoryName: repository.name,
        repositoryId: repository.id ?? undefined,
      }),
    });

    const data = await response.json().catch(() => null);
    if (!response.ok) {
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
      ? (data.branches as CodeHubBranchOption[])
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

export async function fetchOffering() {
  const offeringName = state.offeringName.trim();
  if (!offeringName) {
    state = {
      ...state,
      error: "Please enter an offering name.",
    };
    notify();
    return;
  }

  state = {
    ...state,
    isLoadingOffering: true,
    error: null,
    warning: null,
    offeringId: "",
    teamName: "",
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
    const response = await fetch("/api/codehub/offering", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ offeringName }),
    });

    const data = await response.json().catch(() => null);
    if (!response.ok) {
      state = {
        ...state,
        isLoadingOffering: false,
        error: data?.error ?? "Failed to load offering.",
      };
      notify();
      return;
    }

    const repositories = Array.isArray(data?.repositories)
      ? (data.repositories as CodeHubRepositoryOption[])
      : [];
    const selectedRepositoryName =
      repositories.length === 1 ? repositories[0].name : "";

    state = {
      ...state,
      isLoadingOffering: false,
      offeringId: typeof data?.offeringId === "string" ? data.offeringId : "",
      teamName: typeof data?.teamName === "string" ? data.teamName : offeringName,
      repositories,
      selectedRepositoryName,
      warning:
        repositories.length === 0
          ? "This offering does not have any repositories yet."
          : null,
    };
    notify();

    if (selectedRepositoryName) {
      await fetchBranchesForRepository(selectedRepositoryName);
    }
  } catch {
    state = {
      ...state,
      isLoadingOffering: false,
      error: "Network error while loading the offering.",
    };
    notify();
  }
}

export async function selectRepository(repositoryName: string) {
  const nextRepository =
    state.repositories.find((repository) => repository.name === repositoryName) ?? null;

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
  };
  notify();

  if (nextRepository) {
    await fetchBranchesForRepository(nextRepository.name);
  }
}

export async function fetchTree() {
  const repository = getSelectedRepository();
  if (!state.offeringId || !repository) {
    state = {
      ...state,
      error: "Please load an offering and select a repository.",
    };
    notify();
    return;
  }

  state = {
    ...state,
    isLoadingTree: true,
    error: null,
    warning: state.warning,
    tree: [],
    truncated: false,
    selectedPaths: new Set(),
    fetchedFiles: [],
  };
  notify();

  try {
    const response = await fetch("/api/codehub/tree", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        offeringId: state.offeringId,
        repositoryName: repository.name,
        repositoryId: repository.id ?? undefined,
        branch: state.branch || undefined,
      }),
    });

    const data = await response.json().catch(() => null);
    if (!response.ok) {
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
  const repository = getSelectedRepository();
  if (!state.offeringId || !repository || state.selectedPaths.size === 0) {
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
  };
  notify();

  try {
    const response = await fetch("/api/codehub/files", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        offeringId: state.offeringId,
        repositoryName: repository.name,
        repositoryId: repository.id ?? undefined,
        branch: state.branch || state.defaultBranch || undefined,
        files: filesToFetch,
      }),
    });

    const data = await response.json().catch(() => null);
    if (!response.ok) {
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

export function resetCodeHubImport() {
  state = createInitialState();
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

export function useCodeHubImportState(): CodeHubImportState {
  const [current, setCurrent] = React.useState<CodeHubImportState>(getCodeHubImportState);

  React.useEffect(() => {
    return subscribeCodeHubImport(() => {
      setCurrent(getCodeHubImportState());
    });
  }, []);

  return current;
}
