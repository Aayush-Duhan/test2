"use client";

import * as React from "react";
import {
  Folder,
  FolderOpen,
  File,
  ChevronRight,
  ChevronDown,
  CheckSquare,
  Square,
} from "lucide-react";

// ── Types ──────────────────────────────────────────────────────────────────────

export interface FlatFileEntry {
  path: string;
  sha?: string;
  size?: number;
}

interface TreeNode {
  name: string;
  path: string;
  isFolder: boolean;
  children: TreeNode[];
  size?: number;
}

interface FileTreeProps {
  entries: FlatFileEntry[];
  selectedPaths: Set<string>;
  onToggle: (path: string) => void;
  searchQuery?: string;
  className?: string;
}

interface TreeNodeRowProps {
  node: TreeNode;
  depth: number;
  selectedPaths: Set<string>;
  onToggle: (path: string) => void;
  searchQuery: string;
  activeGuides: Set<number>;
  isLast: boolean;
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function buildTree(entries: FlatFileEntry[]): TreeNode[] {
  const root: TreeNode = { name: "", path: "", isFolder: true, children: [] };

  for (const entry of entries) {
    const parts = entry.path.split("/");
    let current = root;

    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      const isFile = i === parts.length - 1;
      const existing = current.children.find(
        (c) => c.name === part && c.isFolder === !isFile
      );

      if (existing) {
        current = existing;
      } else {
        const node: TreeNode = {
          name: part,
          path: isFile ? entry.path : parts.slice(0, i + 1).join("/"),
          isFolder: !isFile,
          children: [],
          size: isFile ? entry.size : undefined,
        };
        current.children.push(node);
        current = node;
      }
    }
  }

  function sortChildren(node: TreeNode) {
    node.children.sort((a, b) => {
      if (a.isFolder !== b.isFolder) return a.isFolder ? -1 : 1;
      return a.name.localeCompare(b.name);
    });
    node.children.forEach(sortChildren);
  }
  sortChildren(root);
  return root.children;
}

function HighlightMatch({ text, query }: { text: string; query: string }) {
  if (!query) return <>{text}</>;
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx === -1) return <>{text}</>;
  return (
    <>
      {text.slice(0, idx)}
      <span className="bg-neutral-500/30 text-white rounded-sm px-0.5">
        {text.slice(idx, idx + query.length)}
      </span>
      {text.slice(idx + query.length)}
    </>
  );
}

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function getAllFilePaths(node: TreeNode): string[] {
  if (!node.isFolder) return [node.path];
  return node.children.flatMap(getAllFilePaths);
}

// ── TreeNodeRow ────────────────────────────────────────────────────────────────

const TreeNodeRow = React.memo(function TreeNodeRow({
  node,
  depth,
  selectedPaths,
  onToggle,
  searchQuery,
  activeGuides,
  isLast,
}: TreeNodeRowProps) {
  const [expanded, setExpanded] = React.useState(depth === 0 || !!searchQuery);

  React.useEffect(() => {
    if (searchQuery) setExpanded(true);
  }, [searchQuery]);

  const filePaths = React.useMemo(
    () => (node.isFolder ? getAllFilePaths(node) : []),
    [node]
  );

  const allChildrenSelected =
    node.isFolder &&
    filePaths.length > 0 &&
    filePaths.every((p) => selectedPaths.has(p));

  const someChildrenSelected =
    node.isFolder &&
    !allChildrenSelected &&
    filePaths.some((p) => selectedPaths.has(p));

  const isFileSelected = !node.isFolder && selectedPaths.has(node.path);

  const handleToggleFolder = (e: React.MouseEvent) => {
    e.stopPropagation();
    setExpanded(!expanded);
  };

  const handleToggleSelection = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (node.isFolder) {
      if (allChildrenSelected) {
        filePaths.forEach((p) => onToggle(p));
      } else {
        filePaths.filter((p) => !selectedPaths.has(p)).forEach((p) => onToggle(p));
      }
    } else {
      onToggle(node.path);
    }
  };

  const handleRowClick = () => {
    if (node.isFolder) setExpanded(!expanded);
    else onToggle(node.path);
  };

  const childGuides = React.useMemo(() => {
    const next = new Set(activeGuides);
    if (!isLast) next.add(depth);
    else next.delete(depth);
    return next;
  }, [activeGuides, depth, isLast]);

  return (
    <>
      <button
        type="button"
        onClick={handleRowClick}
        className={[
          "group relative flex w-full items-center text-left transition-colors duration-75",
          "py-[5px] pr-4",
          !node.isFolder && isFileSelected
            ? "bg-neutral-700/20"
            : "hover:bg-neutral-800/50",
        ]
          .filter(Boolean)
          .join(" ")}
        style={{ paddingLeft: `${depth * 18 + 10}px` }}
      >
        {/* Indent guides */}
        {Array.from({ length: depth }).map((_, i) => (
          <span
            key={i}
            className="absolute top-0 bottom-0 w-px"
            style={{
              left: `${i * 18 + 18}px`,
              backgroundColor: activeGuides.has(i)
                ? "rgba(255,255,255,0.05)"
                : "transparent",
            }}
          />
        ))}

        {/* Chevron */}
        {node.isFolder ? (
          <span
            onClick={handleToggleFolder}
            className="mr-1 flex h-4 w-4 shrink-0 items-center justify-center text-neutral-500 group-hover:text-neutral-300"
          >
            {expanded ? (
              <ChevronDown className="h-3 w-3" />
            ) : (
              <ChevronRight className="h-3 w-3" />
            )}
          </span>
        ) : (
          <span className="mr-1 w-4 shrink-0" />
        )}

        {/* Checkbox */}
        <span onClick={handleToggleSelection} className="mr-2 flex shrink-0 items-center">
          {(node.isFolder ? allChildrenSelected : isFileSelected) ? (
            <CheckSquare className="h-3.5 w-3.5 text-neutral-200" />
          ) : someChildrenSelected ? (
            <span className="relative flex h-3.5 w-3.5 items-center justify-center">
              <Square className="h-3.5 w-3.5 text-neutral-500" />
              <span className="absolute h-1.5 w-1.5 rounded-[1px] bg-neutral-500" />
            </span>
          ) : (
            <Square className="h-3.5 w-3.5 text-neutral-600" />
          )}
        </span>

        {/* Icon */}
        {node.isFolder ? (
          expanded ? (
            <FolderOpen className="mr-2 h-4 w-4 shrink-0 text-neutral-400" />
          ) : (
            <Folder className="mr-2 h-4 w-4 shrink-0 text-neutral-400" />
          )
        ) : (
          <File className="mr-2 h-3.5 w-3.5 shrink-0 text-neutral-500" />
        )}

        {/* Name */}
        <span
          className={[
            "flex-1 truncate text-[13px]",
            node.isFolder ? "font-medium text-neutral-100" : "",
            !node.isFolder && isFileSelected ? "text-neutral-100" : "",
            !node.isFolder && !isFileSelected ? "text-neutral-400" : "",
          ]
            .filter(Boolean)
            .join(" ")}
        >
          <HighlightMatch text={node.name} query={searchQuery} />
        </span>

        {/* Meta */}
        {!node.isFolder && node.size != null && (
          <span className="ml-2 shrink-0 text-[10px] text-neutral-500">
            {formatFileSize(node.size)}
          </span>
        )}
        {node.isFolder && (
          <span className="ml-2 shrink-0 text-[10px] text-neutral-500">
            {filePaths.length}
          </span>
        )}
      </button>

      {node.isFolder && expanded && (
        <div className="relative">
          {node.children.map((child, i) => (
            <TreeNodeRow
              key={child.path}
              node={child}
              depth={depth + 1}
              selectedPaths={selectedPaths}
              onToggle={onToggle}
              searchQuery={searchQuery}
              activeGuides={childGuides}
              isLast={i === node.children.length - 1}
            />
          ))}
        </div>
      )}
    </>
  );
});

// ── FileTree ───────────────────────────────────────────────────────────────────

export function FileTree({
  entries,
  selectedPaths,
  onToggle,
  searchQuery = "",
  className,
}: FileTreeProps) {
  const tree = React.useMemo(() => buildTree(entries), [entries]);

  if (tree.length === 0) {
    return (
      <div className="flex items-center justify-center py-12">
        <p className="text-sm text-neutral-500">No files to display.</p>
      </div>
    );
  }

  return (
    <div
      className={[
        "max-h-96 overflow-y-auto rounded border border-neutral-800 bg-[#0e0e0e] scrollbar-dark",
        className,
      ]
        .filter(Boolean)
        .join(" ")}
    >
      {tree.map((node, i) => (
        <TreeNodeRow
          key={node.path}
          node={node}
          depth={0}
          selectedPaths={selectedPaths}
          onToggle={onToggle}
          searchQuery={searchQuery}
          activeGuides={new Set()}
          isLast={i === tree.length - 1}
        />
      ))}
    </div>
  );
}

export default FileTree;
