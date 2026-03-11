"use client";

import * as React from "react";
import * as Dialog from "@radix-ui/react-dialog";
import {
  Github,
  Search,
  X,
  Loader2,
  FileText,
  CheckSquare,
  Square,
  AlertTriangle,
  FolderTree,
  Lock,
  ChevronDown,
  ChevronRight,
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

// ── Helpers ────────────────────────────────────────────────────────────────────

function cn(...inputs: (string | boolean | undefined | null)[]): string {
  return inputs.filter(Boolean).join(" ");
}

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

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

  // Get the appropriate extension filter based on mode
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

  // Reset state when modal opens
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

    // Convert GitHub files to WizardFile objects with synthetic File blobs
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

  const title =
    mode === "source"
      ? "Import Source Files from GitHub"
      : "Import Schema Mappings from GitHub";

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm animate-in fade-in-0" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-50 w-[96vw] max-w-[720px] -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-[#333] bg-[#0d0d0d] shadow-2xl shadow-black/40 animate-in fade-in-0 zoom-in-95 focus:outline-none">
          {/* Header */}
          <div className="flex items-center justify-between border-b border-[#222] px-6 py-4">
            <Dialog.Title className="flex items-center gap-3 text-lg font-semibold text-white">
              <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-[#4da5fc]/10 border border-[#4da5fc]/20">
                <Github className="h-5 w-5 text-[#4da5fc]" />
              </div>
              {title}
            </Dialog.Title>
            <Dialog.Close className="rounded-full p-1.5 text-[#666] transition-colors hover:bg-[#222] hover:text-white">
              <X className="h-5 w-5" />
            </Dialog.Close>
          </div>

          <div className="px-6 py-5 space-y-5 max-h-[70vh] overflow-y-auto scrollbar-dark">
            {/* Repository URL Input */}
            <div className="space-y-3">
              <div className="space-y-1.5">
                <label className="text-xs font-medium text-[#8a8a8f]">
                  Repository URL
                </label>
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={ghState.repoUrl}
                    onChange={(e) => setRepoUrl(e.target.value)}
                    placeholder="https://github.com/owner/repo  or  owner/repo"
                    className="flex-1 rounded-lg border border-[#333] bg-[#1a1a1a] px-3 py-2.5 text-sm text-white placeholder-[#555] outline-none transition-colors focus:border-[#4da5fc] focus:ring-1 focus:ring-[#4da5fc]/30"
                    onKeyDown={(e) => {
                      if (e.key === "Enter" && ghState.owner && ghState.repo) {
                        void handleLoadRepo();
                      }
                    }}
                  />
                  <button
                    type="button"
                    onClick={() => void handleLoadRepo()}
                    disabled={
                      !ghState.owner ||
                      !ghState.repo ||
                      ghState.isLoadingTree
                    }
                    className={cn(
                      "flex items-center gap-2 rounded-lg px-4 py-2.5 text-sm font-medium transition-all whitespace-nowrap",
                      ghState.owner && ghState.repo && !ghState.isLoadingTree
                        ? "bg-[#4da5fc] text-white hover:bg-[#3d8fd6]"
                        : "bg-[#333] text-[#666] cursor-not-allowed"
                    )}
                  >
                    {ghState.isLoadingTree ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <FolderTree className="h-4 w-4" />
                    )}
                    {ghState.isLoadingTree ? "Loading…" : "Load Repo"}
                  </button>
                </div>
              </div>

              {/* Optional fields toggle */}
              <div className="flex gap-3">
                <button
                  type="button"
                  onClick={() => setShowTokenField(!showTokenField)}
                  className="flex items-center gap-1.5 text-xs text-[#666] hover:text-[#8a8a8f] transition-colors"
                >
                  <Lock className="h-3 w-3" />
                  {showTokenField ? "Hide" : "Private repo?"}
                  {showTokenField ? (
                    <ChevronDown className="h-3 w-3" />
                  ) : (
                    <ChevronRight className="h-3 w-3" />
                  )}
                </button>
                <button
                  type="button"
                  onClick={() => setShowBranchField(!showBranchField)}
                  className="flex items-center gap-1.5 text-xs text-[#666] hover:text-[#8a8a8f] transition-colors"
                >
                  Custom branch
                  {showBranchField ? (
                    <ChevronDown className="h-3 w-3" />
                  ) : (
                    <ChevronRight className="h-3 w-3" />
                  )}
                </button>
              </div>

              {/* Token field */}
              {showTokenField && (
                <div className="space-y-1.5">
                  <label className="text-xs font-medium text-[#8a8a8f] flex items-center gap-1">
                    <Lock className="h-3 w-3" />
                    Personal Access Token
                    <span className="text-[#555]">(optional)</span>
                  </label>
                  <input
                    type="password"
                    value={ghState.token}
                    onChange={(e) => setToken(e.target.value)}
                    placeholder="ghp_xxxxxxxxxxxxxxxxxxxx"
                    className="w-full rounded-lg border border-[#333] bg-[#1a1a1a] px-3 py-2.5 text-sm text-white placeholder-[#555] outline-none transition-colors focus:border-[#4da5fc] focus:ring-1 focus:ring-[#4da5fc]/30"
                  />
                  <p className="text-[10px] text-[#555]">
                    Required for private repositories. Token is only used for this request and never stored.
                  </p>
                </div>
              )}

              {/* Branch field */}
              {showBranchField && (
                <div className="space-y-1.5">
                  <label className="text-xs font-medium text-[#8a8a8f]">
                    Branch
                    <span className="text-[#555] ml-1">(defaults to repo default)</span>
                  </label>
                  <input
                    type="text"
                    value={ghState.branch}
                    onChange={(e) => setBranch(e.target.value)}
                    placeholder="main"
                    className="w-full rounded-lg border border-[#333] bg-[#1a1a1a] px-3 py-2.5 text-sm text-white placeholder-[#555] outline-none transition-colors focus:border-[#4da5fc] focus:ring-1 focus:ring-[#4da5fc]/30"
                  />
                </div>
              )}
            </div>

            {/* Error */}
            {ghState.error && (
              <div className="flex items-start gap-2 rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-red-400" />
                <p className="text-sm text-red-300">{ghState.error}</p>
              </div>
            )}

            {/* File tree */}
            {ghState.tree.length > 0 && (
              <div className="space-y-3">
                {/* Truncation warning */}
                {ghState.truncated && (
                  <div className="flex items-start gap-2 rounded-lg border border-amber-400/30 bg-amber-500/10 px-3 py-2">
                    <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-400" />
                    <p className="text-xs text-amber-200">
                      This repository is very large. The file list may be incomplete (truncated by GitHub).
                    </p>
                  </div>
                )}

                {/* Search + select all row */}
                <div className="flex items-center gap-2">
                  <div className="relative flex-1">
                    <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[#555]" />
                    <input
                      type="text"
                      value={ghState.searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      placeholder="Filter files…"
                      className="w-full rounded-lg border border-[#333] bg-[#1a1a1a] pl-9 pr-3 py-2 text-sm text-white placeholder-[#555] outline-none transition-colors focus:border-[#4da5fc] focus:ring-1 focus:ring-[#4da5fc]/30"
                    />
                  </div>
                  <button
                    type="button"
                    onClick={handleToggleAll}
                    className="flex items-center gap-1.5 whitespace-nowrap rounded-lg border border-[#333] bg-[#1a1a1a] px-3 py-2 text-xs text-[#8a8a8f] transition-colors hover:border-[#444] hover:text-white"
                  >
                    {allVisibleSelected ? (
                      <CheckSquare className="h-3.5 w-3.5 text-[#4da5fc]" />
                    ) : (
                      <Square className="h-3.5 w-3.5" />
                    )}
                    {allVisibleSelected ? "Deselect all" : "Select all"}
                  </button>
                </div>

                {/* Stats bar */}
                <div className="flex items-center justify-between text-xs text-[#666]">
                  <span>
                    {filteredTree.length} file{filteredTree.length !== 1 ? "s" : ""} shown
                    {ghState.searchQuery && ` (filtered from ${ghState.tree.length})`}
                  </span>
                  <span className="text-[#4da5fc]">
                    {ghState.selectedPaths.size} selected
                  </span>
                </div>

                {/* File list */}
                <div className="max-h-64 overflow-y-auto rounded-lg border border-[#222] bg-[#111] scrollbar-dark">
                  {filteredTree.length === 0 ? (
                    <div className="flex items-center justify-center py-8">
                      <p className="text-sm text-[#555]">
                        No files match your filter.
                      </p>
                    </div>
                  ) : (
                    filteredTree.map((entry) => {
                      const isSelected = ghState.selectedPaths.has(entry.path);
                      return (
                        <button
                          type="button"
                          key={entry.sha}
                          onClick={() => toggleFile(entry.path)}
                          className={cn(
                            "flex w-full items-center gap-3 border-b border-[#1a1a1a] px-3 py-2 text-left transition-colors last:border-b-0",
                            isSelected
                              ? "bg-[#4da5fc]/5 hover:bg-[#4da5fc]/10"
                              : "hover:bg-[#1a1a1a]"
                          )}
                        >
                          {isSelected ? (
                            <CheckSquare className="h-4 w-4 shrink-0 text-[#4da5fc]" />
                          ) : (
                            <Square className="h-4 w-4 shrink-0 text-[#444]" />
                          )}
                          <FileText className="h-3.5 w-3.5 shrink-0 text-[#666]" />
                          <span
                            className={cn(
                              "flex-1 truncate text-sm",
                              isSelected ? "text-white" : "text-[#999]"
                            )}
                          >
                            {entry.path}
                          </span>
                          <span className="shrink-0 text-[10px] text-[#555]">
                            {formatFileSize(entry.size)}
                          </span>
                        </button>
                      );
                    })
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between border-t border-[#222] px-6 py-4">
            <Dialog.Close asChild>
              <button
                type="button"
                className="rounded-lg px-4 py-2 text-sm text-[#8a8a8f] transition-colors hover:bg-[#1a1a1a] hover:text-white"
              >
                Cancel
              </button>
            </Dialog.Close>

            {ghState.tree.length > 0 && (
              <button
                type="button"
                onClick={() => void handleImport()}
                disabled={
                  ghState.selectedPaths.size === 0 || ghState.isLoadingFiles
                }
                className={cn(
                  "flex items-center gap-2 rounded-lg px-5 py-2 text-sm font-medium transition-all",
                  ghState.selectedPaths.size > 0 && !ghState.isLoadingFiles
                    ? "bg-[#4da5fc] text-white hover:bg-[#3d8fd6]"
                    : "bg-[#333] text-[#666] cursor-not-allowed"
                )}
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
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
