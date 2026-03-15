"use client";

import * as React from "react";
import * as Dialog from "@radix-ui/react-dialog";
import { FileTree } from "@/components/ui/file-tree";
import {
  Github,
  Search,
  X,
  Loader2,
  CheckSquare,
  Square,
  AlertTriangle,
  ArrowRight,
  ChevronDown,
  FolderGit2,
  GitBranch,
  Building2,
} from "lucide-react";
import {
  useCodeHubImportState,
  setOfferingName,
  setBranch,
  setSearchQuery,
  toggleFile,
  selectAllVisible,
  deselectAllVisible,
  fetchOffering,
  selectRepository,
  fetchTree,
  fetchSelectedFiles,
  resetCodeHubImport,
  dismissWarning,
  getFilteredTree,
  type RepoFetchedFile,
} from "@/lib/codehub-store";
import type { WizardFile } from "@/lib/wizard-store";

export type CodeHubImportMode = "source" | "mapping";

interface CodeHubImportModalProps {
  mode: CodeHubImportMode;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onImport: (files: WizardFile[]) => void;
}

export function CodeHubImportModal({
  mode,
  open,
  onOpenChange,
  onImport,
}: CodeHubImportModalProps) {
  const importState = useCodeHubImportState();

  const extensionFilter = React.useMemo(
    () => (mode === "source" ? undefined : ["csv", "json"]),
    [mode]
  );

  const filteredTree = getFilteredTree(extensionFilter);

  const visiblePaths = React.useMemo(
    () => filteredTree.map((entry) => entry.path),
    [filteredTree]
  );

  const allVisibleSelected =
    visiblePaths.length > 0 &&
    visiblePaths.every((path) => importState.selectedPaths.has(path));

  React.useEffect(() => {
    if (open) {
      resetCodeHubImport();
    }
  }, [open]);

  const handleLoadOffering = async () => {
    await fetchOffering();
  };

  const handleLoadRepository = async () => {
    await fetchTree();
  };

  const handleImport = async () => {
    const result = await fetchSelectedFiles();
    if (result.files.length === 0) return;

    const wizardFiles: WizardFile[] = result.files.map((file: RepoFetchedFile) => ({
      name: file.path.split("/").pop() ?? file.path,
      path: file.path,
      relativePath: file.path,
      file: new File([file.content], file.path.split("/").pop() ?? file.path, {
        type: "text/plain",
      }),
    }));

    onImport(wizardFiles);

    if (!result.errors || result.errors.length === 0) {
      onOpenChange(false);
    }
  };

  const handleToggleAll = () => {
    if (allVisibleSelected) {
      deselectAllVisible(visiblePaths);
    } else {
      selectAllVisible(visiblePaths);
    }
  };

  const subtitle =
    mode === "source"
      ? "Select source files from GitHub Enterprise"
      : "Select schema mapping files from GitHub Enterprise";

  const hasTree = importState.tree.length > 0;
  const hasRepositories = importState.repositories.length > 0;
  const showBranchSelector =
    importState.isLoadingBranches || importState.availableBranches.length > 0;

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-black/70" />
        <Dialog.Content className="fixed inset-0 z-50 flex flex-col bg-[#111] focus:outline-none">
          <header className="flex shrink-0 items-center justify-between border-b border-neutral-800 px-6 py-4">
            <div className="flex items-center gap-3">
              <Github className="h-5 w-5 text-neutral-300" />
              <Dialog.Title className="text-sm font-medium text-neutral-100">
                Import from GitHub Enterprise
              </Dialog.Title>
              <span className="text-xs text-neutral-400">{subtitle}</span>
            </div>
            <Dialog.Close className="rounded p-1.5 text-neutral-400 transition-colors hover:bg-neutral-800 hover:text-neutral-100">
              <X className="h-4 w-4" />
            </Dialog.Close>
          </header>

          <div className="flex flex-1 overflow-hidden">
            <aside className="flex w-80 shrink-0 flex-col border-r border-neutral-800 bg-[#0e0e0e]">
              <div className="flex-1 space-y-5 overflow-y-auto p-5 scrollbar-dark">
                <div className="space-y-2">
                  <label className="block text-xs font-medium uppercase tracking-wider text-neutral-400">
                    Offering name
                  </label>
                  <input
                    type="text"
                    value={importState.offeringName}
                    onChange={(event) => setOfferingName(event.target.value)}
                    placeholder="demo-test"
                    className="w-full rounded border border-neutral-700 bg-transparent px-3 py-2 text-sm text-neutral-100 placeholder-neutral-500 outline-none transition-colors focus:border-neutral-500"
                    onKeyDown={(event) => {
                      if (event.key === "Enter" && importState.offeringName.trim()) {
                        void handleLoadOffering();
                      }
                    }}
                  />
                </div>

                <button
                  type="button"
                  onClick={() => void handleLoadOffering()}
                  disabled={!importState.offeringName.trim() || importState.isLoadingOffering}
                  className="flex w-full items-center justify-center gap-2 rounded border border-neutral-700 bg-neutral-800 px-4 py-2.5 text-sm font-medium text-neutral-100 transition-colors hover:bg-neutral-700 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  {importState.isLoadingOffering ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <Building2 className="h-4 w-4" />
                  )}
                  {importState.isLoadingOffering ? "Loading..." : "Load offering"}
                </button>

                {importState.offeringId && (
                  <div className="rounded border border-neutral-800 bg-neutral-900/70 px-3 py-2.5">
                    <p className="text-[11px] uppercase tracking-wider text-neutral-500">
                      Offering
                    </p>
                    <p className="mt-1 text-sm text-neutral-100">
                      {importState.teamName || importState.offeringName}
                    </p>
                    <p className="mt-1 text-xs text-neutral-500">
                      {importState.repositories.length} repos available
                    </p>
                  </div>
                )}

                {hasRepositories && (
                  <div className="space-y-2">
                    <label className="block text-xs font-medium uppercase tracking-wider text-neutral-400">
                      Repository
                    </label>
                    <div className="relative">
                      <FolderGit2 className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-neutral-500" />
                      <select
                        value={importState.selectedRepositoryName}
                        onChange={(event) => void selectRepository(event.target.value)}
                        className="w-full appearance-none rounded border border-neutral-700 bg-transparent py-2 pl-9 pr-10 text-sm text-neutral-100 outline-none transition-colors focus:border-neutral-500"
                      >
                        <option value="" className="bg-neutral-900 text-neutral-400">
                          Select a repository
                        </option>
                        {importState.repositories.map((repository) => (
                          <option
                            key={repository.name}
                            value={repository.name}
                            className="bg-neutral-900 text-neutral-100"
                          >
                            {repository.name}
                          </option>
                        ))}
                      </select>
                      <ChevronDown className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-neutral-500" />
                    </div>
                  </div>
                )}

                {showBranchSelector && (
                  <div className="space-y-2">
                    <label className="block text-xs font-medium uppercase tracking-wider text-neutral-400">
                      Branch
                    </label>
                    <div className="relative">
                      <GitBranch className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-neutral-500" />
                      <select
                        value={importState.branch}
                        onChange={(event) => setBranch(event.target.value)}
                        disabled={
                          importState.isLoadingBranches ||
                          importState.availableBranches.length === 0
                        }
                        className="w-full appearance-none rounded border border-neutral-700 bg-transparent py-2 pl-9 pr-10 text-sm text-neutral-100 outline-none transition-colors focus:border-neutral-500 disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        {importState.isLoadingBranches ? (
                          <option value="" className="bg-neutral-900 text-neutral-100">
                            Loading branches...
                          </option>
                        ) : (
                          importState.availableBranches.map((branch) => (
                            <option
                              key={branch.name}
                              value={branch.name}
                              className="bg-neutral-900 text-neutral-100"
                            >
                              {branch.name}
                              {branch.isDefault ? " (default)" : ""}
                            </option>
                          ))
                        )}
                      </select>
                      <ChevronDown className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-neutral-500" />
                    </div>
                  </div>
                )}

                <button
                  type="button"
                  onClick={() => void handleLoadRepository()}
                  disabled={
                    !importState.offeringId ||
                    !importState.selectedRepositoryName ||
                    importState.isLoadingTree
                  }
                  className="flex w-full items-center justify-center gap-2 rounded border border-neutral-700 bg-neutral-800 px-4 py-2.5 text-sm font-medium text-neutral-100 transition-colors hover:bg-neutral-700 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  {importState.isLoadingTree ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <ArrowRight className="h-4 w-4" />
                  )}
                  {importState.isLoadingTree ? "Loading..." : "Load repository"}
                </button>

                {importState.error && (
                  <div className="flex items-start gap-2 rounded border border-red-900/50 bg-red-950/30 px-3 py-2.5">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-red-400" />
                    <p className="text-xs leading-relaxed text-red-300">
                      {importState.error}
                    </p>
                  </div>
                )}

                {importState.warning && (
                  <div className="rounded border border-amber-900/50 bg-amber-950/30 px-3 py-2.5">
                    <div className="flex items-start gap-2">
                      <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-400" />
                      <p className="flex-1 text-xs leading-relaxed text-amber-300/90">
                        {importState.warning}
                      </p>
                      <button
                        type="button"
                        onClick={dismissWarning}
                        className="text-[10px] uppercase tracking-wider text-amber-200/80 transition-colors hover:text-amber-100"
                      >
                        Dismiss
                      </button>
                    </div>
                  </div>
                )}

                {importState.truncated && (
                  <div className="flex items-start gap-2 rounded border border-amber-900/50 bg-amber-950/30 px-3 py-2.5">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-400" />
                    <p className="text-xs leading-relaxed text-amber-300/80">
                      Large repository. The file list may be incomplete.
                    </p>
                  </div>
                )}
              </div>
            </aside>

            <main className="flex flex-1 flex-col overflow-hidden">
              {hasTree ? (
                <>
                  <div className="flex shrink-0 items-center gap-3 border-b border-neutral-800 px-5 py-3">
                    <div className="relative flex-1">
                      <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-neutral-600" />
                      <input
                        type="text"
                        value={importState.searchQuery}
                        onChange={(event) => setSearchQuery(event.target.value)}
                        placeholder="Filter files..."
                        className="w-full rounded border border-neutral-700 bg-transparent py-1.5 pl-8 pr-3 text-sm text-neutral-100 placeholder-neutral-500 outline-none transition-colors focus:border-neutral-500"
                      />
                    </div>
                    <button
                      type="button"
                      onClick={handleToggleAll}
                      className="flex items-center gap-1.5 whitespace-nowrap rounded border border-neutral-700 px-2.5 py-1.5 text-xs text-neutral-300 transition-colors hover:bg-neutral-800 hover:text-neutral-100"
                    >
                      {allVisibleSelected ? (
                        <CheckSquare className="h-3.5 w-3.5" />
                      ) : (
                        <Square className="h-3.5 w-3.5" />
                      )}
                      {allVisibleSelected ? "Deselect all" : "Select all"}
                    </button>
                  </div>

                  <div className="flex shrink-0 items-center justify-between border-b border-neutral-800/50 px-5 py-1.5 text-[11px] text-neutral-400">
                    <span>
                      {filteredTree.length} file{filteredTree.length !== 1 ? "s" : ""}
                      {importState.searchQuery && ` of ${importState.tree.length}`}
                    </span>
                    <span className="text-neutral-300">
                      {importState.selectedPaths.size} selected
                    </span>
                  </div>

                  <div className="flex-1 overflow-y-auto scrollbar-dark">
                    <FileTree
                      entries={filteredTree}
                      selectedPaths={importState.selectedPaths}
                      onToggle={toggleFile}
                      searchQuery={importState.searchQuery}
                      className="max-h-none rounded-none border-0"
                    />
                  </div>
                </>
              ) : (
                <div className="flex flex-1 items-center justify-center">
                  <div className="space-y-2 text-center">
                    <Github className="mx-auto h-8 w-8 text-neutral-600" />
                    <p className="text-sm text-neutral-400">
                      {importState.offeringId
                        ? hasRepositories
                          ? "Select a repo, choose a branch if needed, and load it to browse files."
                          : "This offering does not have repositories to import from."
                        : "Enter an offering name to browse repositories from GitHub Enterprise."}
                    </p>
                  </div>
                </div>
              )}
            </main>
          </div>

          <footer className="flex shrink-0 items-center justify-between border-t border-neutral-800 px-6 py-3">
            <Dialog.Close asChild>
              <button
                type="button"
                className="rounded px-4 py-2 text-sm text-neutral-400 transition-colors hover:bg-neutral-800 hover:text-neutral-100"
              >
                Cancel
              </button>
            </Dialog.Close>

            {hasTree && (
              <button
                type="button"
                onClick={() => void handleImport()}
                disabled={
                  importState.selectedPaths.size === 0 || importState.isLoadingFiles
                }
                className="flex items-center gap-2 rounded bg-neutral-200 px-5 py-2 text-sm font-medium text-neutral-900 transition-colors hover:bg-white disabled:cursor-not-allowed disabled:opacity-30"
              >
                {importState.isLoadingFiles ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Importing...
                  </>
                ) : (
                  <>
                    Import {importState.selectedPaths.size} file
                    {importState.selectedPaths.size !== 1 ? "s" : ""}
                  </>
                )}
              </button>
            )}
          </footer>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
