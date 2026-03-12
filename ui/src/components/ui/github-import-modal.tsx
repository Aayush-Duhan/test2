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
  Lock,
  ChevronDown,
  ChevronRight,
  GitBranch,
} from "lucide-react";
import {
  useGitHubImportState,
  setRepoUrl,
  setToken,
  setBranch,
  setSearchQuery,
  toggleFile,
  selectAllVisible,
  deselectAllVisible,
  fetchTree,
  fetchSelectedFiles,
  resetGitHubImport,
  getFilteredTree,
  type GitHubFetchedFile,
} from "@/lib/github-store";
import type { WizardFile } from "@/lib/wizard-store";

// ── Props ──────────────────────────────────────────────────────────────────────

export type GitHubImportMode = "source" | "mapping";

interface GitHubImportModalProps {
  mode: GitHubImportMode;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onImport: (files: WizardFile[]) => void;
}

// ── Main Component ─────────────────────────────────────────────────────────────

export function GitHubImportModal({
  mode,
  open,
  onOpenChange,
  onImport,
}: GitHubImportModalProps) {
  const ghState = useGitHubImportState();
  const [showTokenField, setShowTokenField] = React.useState(false);
  const [showBranchField, setShowBranchField] = React.useState(false);

  const extensionFilter = React.useMemo(
    () => (mode === "source" ? undefined : ["csv", "json"]),
    [mode]
  );

  const filteredTree = React.useMemo(
    () => getFilteredTree(extensionFilter),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [ghState.tree, ghState.searchQuery, extensionFilter]
  );

  const visiblePaths = React.useMemo(
    () => filteredTree.map((e) => e.path),
    [filteredTree]
  );

  const allVisibleSelected =
    visiblePaths.length > 0 &&
    visiblePaths.every((p) => ghState.selectedPaths.has(p));

  React.useEffect(() => {
    if (open) {
      resetGitHubImport();
      setShowTokenField(false);
      setShowBranchField(false);
    }
  }, [open]);

  const handleLoadRepo = async () => {
    await fetchTree();
  };

  const handleImport = async () => {
    const fetched = await fetchSelectedFiles();
    if (fetched.length === 0) return;

    const wizardFiles: WizardFile[] = fetched.map(
      (f: GitHubFetchedFile) => ({
        name: f.path.split("/").pop() ?? f.path,
        path: f.path,
        relativePath: f.path,
        file: new File([f.content], f.path.split("/").pop() ?? f.path, {
          type: "text/plain",
        }),
      })
    );

    onImport(wizardFiles);
    onOpenChange(false);
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
      ? "Select source files to import"
      : "Select schema mapping files";

  const hasTree = ghState.tree.length > 0;

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-black/70" />
        <Dialog.Content className="fixed inset-0 z-50 flex flex-col bg-[#111] focus:outline-none">
          {/* ── Top bar ──────────────────────────────────────────────── */}
          <header className="flex items-center justify-between border-b border-neutral-800 px-6 py-4 shrink-0">
            <div className="flex items-center gap-3">
              <Github className="h-5 w-5 text-neutral-300" />
              <Dialog.Title className="text-sm font-medium text-neutral-100">
                Import from GitHub
              </Dialog.Title>
              <span className="text-xs text-neutral-400">{subtitle}</span>
            </div>
            <Dialog.Close className="rounded p-1.5 text-neutral-400 transition-colors hover:bg-neutral-800 hover:text-neutral-100">
              <X className="h-4 w-4" />
            </Dialog.Close>
          </header>

          {/* ── Body ─────────────────────────────────────────────────── */}
          <div className="flex flex-1 overflow-hidden">
            {/* Left sidebar — repo config */}
            <aside className="flex w-80 shrink-0 flex-col border-r border-neutral-800 bg-[#0e0e0e]">
              <div className="flex-1 overflow-y-auto p-5 space-y-5 scrollbar-dark">
                {/* Repo URL */}
                <div className="space-y-2">
                  <label className="block text-xs font-medium text-neutral-400 uppercase tracking-wider">
                    Repository
                  </label>
                  <input
                    type="text"
                    value={ghState.repoUrl}
                    onChange={(e) => setRepoUrl(e.target.value)}
                    placeholder="owner/repo"
                    className="w-full rounded border border-neutral-700 bg-transparent px-3 py-2 text-sm text-neutral-100 placeholder-neutral-500 outline-none transition-colors focus:border-neutral-500"
                    onKeyDown={(e) => {
                      if (e.key === "Enter" && ghState.owner && ghState.repo) {
                        void handleLoadRepo();
                      }
                    }}
                  />
                </div>

                {/* Optional toggles */}
                <div className="space-y-1">
                  <button
                    type="button"
                    onClick={() => setShowTokenField(!showTokenField)}
                    className="flex w-full items-center gap-2 rounded px-2 py-1.5 text-xs text-neutral-400 transition-colors hover:bg-neutral-800 hover:text-neutral-100"
                  >
                    <Lock className="h-3 w-3" />
                    <span className="flex-1 text-left">Private repository</span>
                    {showTokenField ? (
                      <ChevronDown className="h-3 w-3" />
                    ) : (
                      <ChevronRight className="h-3 w-3" />
                    )}
                  </button>

                  {showTokenField && (
                    <div className="pl-7 pr-2 pb-2 space-y-1.5">
                      <input
                        type="password"
                        value={ghState.token}
                        onChange={(e) => setToken(e.target.value)}
                        placeholder="ghp_…"
                        className="w-full rounded border border-neutral-700 bg-transparent px-3 py-2 text-sm text-neutral-100 placeholder-neutral-500 outline-none transition-colors focus:border-neutral-500"
                      />
                      <p className="text-[10px] text-neutral-500 leading-tight">
                        Token is used once and never stored.
                      </p>
                    </div>
                  )}

                  <button
                    type="button"
                    onClick={() => setShowBranchField(!showBranchField)}
                    className="flex w-full items-center gap-2 rounded px-2 py-1.5 text-xs text-neutral-400 transition-colors hover:bg-neutral-800 hover:text-neutral-100"
                  >
                    <GitBranch className="h-3 w-3" />
                    <span className="flex-1 text-left">Custom branch</span>
                    {showBranchField ? (
                      <ChevronDown className="h-3 w-3" />
                    ) : (
                      <ChevronRight className="h-3 w-3" />
                    )}
                  </button>

                  {showBranchField && (
                    <div className="pl-7 pr-2 pb-2">
                      <input
                        type="text"
                        value={ghState.branch}
                        onChange={(e) => setBranch(e.target.value)}
                        placeholder="main"
                        className="w-full rounded border border-neutral-700 bg-transparent px-3 py-2 text-sm text-neutral-100 placeholder-neutral-500 outline-none transition-colors focus:border-neutral-500"
                      />
                    </div>
                  )}
                </div>

                {/* Load button */}
                <button
                  type="button"
                  onClick={() => void handleLoadRepo()}
                  disabled={
                    !ghState.owner || !ghState.repo || ghState.isLoadingTree
                  }
                  className="flex w-full items-center justify-center gap-2 rounded border border-neutral-700 bg-neutral-800 px-4 py-2.5 text-sm font-medium text-neutral-100 transition-colors hover:bg-neutral-700 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {ghState.isLoadingTree ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <ArrowRight className="h-4 w-4" />
                  )}
                  {ghState.isLoadingTree ? "Loading…" : "Load repository"}
                </button>

                {/* Error */}
                {ghState.error && (
                  <div className="flex items-start gap-2 rounded border border-red-900/50 bg-red-950/30 px-3 py-2.5">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-red-400" />
                    <p className="text-xs text-red-300 leading-relaxed">{ghState.error}</p>
                  </div>
                )}

                {/* Truncation warning */}
                {ghState.truncated && (
                  <div className="flex items-start gap-2 rounded border border-amber-900/50 bg-amber-950/30 px-3 py-2.5">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-400" />
                    <p className="text-xs text-amber-300/80 leading-relaxed">
                      Large repository — file list may be incomplete.
                    </p>
                  </div>
                )}
              </div>
            </aside>

            {/* Right panel — file tree */}
            <main className="flex flex-1 flex-col overflow-hidden">
              {hasTree ? (
                <>
                  {/* Toolbar */}
                  <div className="flex items-center gap-3 border-b border-neutral-800 px-5 py-3 shrink-0">
                    <div className="relative flex-1">
                      <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-neutral-600" />
                      <input
                        type="text"
                        value={ghState.searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        placeholder="Filter files…"
                        className="w-full rounded border border-neutral-700 bg-transparent pl-8 pr-3 py-1.5 text-sm text-neutral-100 placeholder-neutral-500 outline-none transition-colors focus:border-neutral-500"
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

                  {/* Stats */}
                  <div className="flex items-center justify-between border-b border-neutral-800/50 px-5 py-1.5 text-[11px] text-neutral-400 shrink-0">
                    <span>
                      {filteredTree.length} file{filteredTree.length !== 1 ? "s" : ""}
                      {ghState.searchQuery && ` of ${ghState.tree.length}`}
                    </span>
                    <span className="text-neutral-300">
                      {ghState.selectedPaths.size} selected
                    </span>
                  </div>

                  {/* Tree */}
                  <div className="flex-1 overflow-y-auto scrollbar-dark">
                    <FileTree
                      entries={filteredTree}
                      selectedPaths={ghState.selectedPaths}
                      onToggle={toggleFile}
                      searchQuery={ghState.searchQuery}
                      className="border-0 rounded-none max-h-none"
                    />
                  </div>
                </>
              ) : (
                <div className="flex flex-1 items-center justify-center">
                  <div className="text-center space-y-2">
                    <Github className="mx-auto h-8 w-8 text-neutral-600" />
                    <p className="text-sm text-neutral-400">
                      Enter a repository URL and load it to browse files
                    </p>
                  </div>
                </div>
              )}
            </main>
          </div>

          {/* ── Bottom bar ───────────────────────────────────────────── */}
          <footer className="flex items-center justify-between border-t border-neutral-800 px-6 py-3 shrink-0">
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
                  ghState.selectedPaths.size === 0 || ghState.isLoadingFiles
                }
                className="flex items-center gap-2 rounded bg-neutral-200 px-5 py-2 text-sm font-medium text-neutral-900 transition-colors hover:bg-white disabled:opacity-30 disabled:cursor-not-allowed"
              >
                {ghState.isLoadingFiles ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Importing…
                  </>
                ) : (
                  <>
                    Import {ghState.selectedPaths.size} file
                    {ghState.selectedPaths.size !== 1 ? "s" : ""}
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
