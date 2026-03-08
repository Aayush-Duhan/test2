"use client";

import * as React from "react";
import { ChevronRight, ChevronDown, FileCode, Circle } from "lucide-react";
import type { FileMap } from "@/lib/workbench-store";
import { cn } from "@/lib/utils";

const NODE_PADDING_LEFT = 8;
const DEFAULT_HIDDEN_FILES = [/\/node_modules\//, /\/\.next/, /\/\.astro/];

interface FileTreeProps {
  files?: FileMap;
  selectedFile?: string;
  onFileSelect?: (filePath: string) => void;
  rootFolder?: string;
  hideRoot?: boolean;
  collapsed?: boolean;
  allowFolderSelection?: boolean;
  hiddenFiles?: Array<string | RegExp>;
  unsavedFiles?: Set<string>;
  className?: string;
}

type Node = FileNode | FolderNode;

interface BaseNode {
  id: number;
  depth: number;
  name: string;
  fullPath: string;
}

interface FileNode extends BaseNode {
  kind: "file";
}

interface FolderNode extends BaseNode {
  kind: "folder";
}

/**
 * Prevents re-render loops by avoiding state updates
 * when the next Set has the same contents.
 */
function setsEqual(a: Set<string>, b: Set<string>) {
  if (a === b) return true;
  if (a.size !== b.size) return false;
  for (const v of a) if (!b.has(v)) return false;
  return true;
}

export function FileTree({
  files = {},
  onFileSelect,
  selectedFile,
  rootFolder = "/",
  hideRoot = false,
  collapsed = false,
  allowFolderSelection = false,
  hiddenFiles,
  className,
  unsavedFiles,
}: FileTreeProps) {
  const computedHiddenFiles = React.useMemo(
    () => [...DEFAULT_HIDDEN_FILES, ...(hiddenFiles ?? [])],
    [hiddenFiles]
  );

  const fileList = React.useMemo(() => {
    return buildFileList(files, rootFolder, hideRoot, computedHiddenFiles);
  }, [files, rootFolder, hideRoot, computedHiddenFiles]);

  /**
   * Derive a stable list of folder full paths and a stable key.
   * This avoids useEffect depending directly on `fileList`,
   * which may change identity frequently even when contents are same.
   */
  const folderPaths = React.useMemo(() => {
    return fileList
      .filter((n) => n.kind === "folder")
      .map((n) => n.fullPath)
      .sort();
  }, [fileList]);

  const folderPathsKey = React.useMemo(() => folderPaths.join("|"), [folderPaths]);

  const [collapsedFolders, setCollapsedFolders] = React.useState<Set<string>>(
    () => (collapsed ? new Set(folderPaths) : new Set())
  );

  /**
   * Keep collapsedFolders in sync with:
   * - `collapsed` prop
   * - folder structure changes (add/remove folders)
   *
   * Crucially: do NOT update state if it doesn't change.
   */
  React.useEffect(() => {
    setCollapsedFolders((prev) => {
      const folderSet = new Set(folderPaths);

      let next: Set<string>;
      if (collapsed) {
        // collapse all folders
        next = folderSet;
      } else {
        // keep only folders that still exist
        next = new Set([...prev].filter((p) => folderSet.has(p)));
      }

      return setsEqual(prev, next) ? prev : next;
    });
  }, [collapsed, folderPaths, folderPathsKey]);

  const filteredFileList = React.useMemo(() => {
    const list: Node[] = [];
    let lastDepth = Number.MAX_SAFE_INTEGER;

    for (const fileOrFolder of fileList) {
      const depth = fileOrFolder.depth;

      if (lastDepth === depth) {
        lastDepth = Number.MAX_SAFE_INTEGER;
      }

      if (collapsedFolders.has(fileOrFolder.fullPath)) {
        lastDepth = Math.min(lastDepth, depth);
      }

      if (lastDepth < depth) {
        continue;
      }

      list.push(fileOrFolder);
    }

    return list;
  }, [fileList, collapsedFolders]);

  const toggleCollapseState = (fullPath: string) => {
    setCollapsedFolders((prevSet) => {
      const newSet = new Set(prevSet);

      if (newSet.has(fullPath)) {
        newSet.delete(fullPath);
      } else {
        newSet.add(fullPath);
      }

      return newSet;
    });
  };

  return (
    <div className={cn("text-sm", className)}>
      {filteredFileList.map((fileOrFolder) => {
        switch (fileOrFolder.kind) {
          case "file": {
            return (
              <FileNodeButton
                key={fileOrFolder.fullPath}
                selected={selectedFile === fileOrFolder.fullPath}
                file={fileOrFolder}
                unsavedChanges={unsavedFiles?.has(fileOrFolder.fullPath)}
                onClick={() => {
                  onFileSelect?.(fileOrFolder.fullPath);
                }}
              />
            );
          }
          case "folder": {
            return (
              <FolderNodeButton
                key={fileOrFolder.fullPath}
                folder={fileOrFolder}
                selected={allowFolderSelection && selectedFile === fileOrFolder.fullPath}
                collapsed={collapsedFolders.has(fileOrFolder.fullPath)}
                onClick={() => {
                  toggleCollapseState(fileOrFolder.fullPath);
                }}
              />
            );
          }
          default: {
            return undefined;
          }
        }
      })}
    </div>
  );
}

interface FolderProps {
  folder: FolderNode;
  collapsed: boolean;
  selected?: boolean;
  onClick: () => void;
}

function FolderNodeButton({
  folder: { depth, name },
  collapsed,
  selected = false,
  onClick,
}: FolderProps) {
  return (
    <NodeButton
      className={cn("group", {
        "bg-transparent text-white/70 hover:text-white hover:bg-white/10": !selected,
        "bg-white/20 text-white": selected,
      })}
      depth={depth}
      icon={collapsed ? <ChevronRight className="scale-98" /> : <ChevronDown className="scale-98" />}
      onClick={onClick}
    >
      {name}
    </NodeButton>
  );
}

interface FileProps {
  file: FileNode;
  selected: boolean;
  unsavedChanges?: boolean;
  onClick: () => void;
}

function FileNodeButton({
  file: { depth, name },
  onClick,
  selected,
  unsavedChanges = false,
}: FileProps) {
  return (
    <NodeButton
      className={cn("group", {
        "bg-transparent hover:bg-white/10 text-white/70": !selected,
        "bg-white/20 text-white": selected,
      })}
      depth={depth}
      icon={
        <FileCode
          className={cn("scale-98", {
            "group-hover:text-white": !selected,
          })}
        />
      }
      onClick={onClick}
    >
      <div
        className={cn("flex items-center", {
          "group-hover:text-white": !selected,
        })}
      >
        <div className="flex-1 truncate pr-2">{name}</div>
        {unsavedChanges && <Circle className="scale-68 shrink-0 text-orange-500" />}
      </div>
    </NodeButton>
  );
}

interface ButtonProps {
  depth: number;
  iconClasses?: string;
  icon?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  onClick?: () => void;
}

function NodeButton({ depth, icon, onClick, className, children }: ButtonProps) {
  return (
    <button
      className={cn(
        "flex items-center gap-1.5 w-full pr-2 border-2 border-transparent text-faded py-0.5",
        className
      )}
      style={{ paddingLeft: `${6 + depth * NODE_PADDING_LEFT}px` }}
      onClick={() => onClick?.()}
      type="button"
    >
      {icon ? <span className={cn("scale-120 shrink-0")}>{icon}</span> : null}
      <div className="truncate w-full text-left">{children}</div>
    </button>
  );
}

function buildFileList(
  files: FileMap,
  rootFolder = "/",
  hideRoot: boolean,
  hiddenFiles: Array<string | RegExp>
): Node[] {
  const folderPaths = new Set<string>();
  const fileList: Node[] = [];

  let defaultDepth = 0;

  if (rootFolder === "/" && !hideRoot) {
    defaultDepth = 1;
    fileList.push({ kind: "folder", name: "/", depth: 0, id: 0, fullPath: "/" });
  }

  for (const [filePath, dirent] of Object.entries(files)) {
    const segments = filePath.split("/").filter((segment) => segment);
    const fileName = segments.at(-1);

    if (!fileName || isHiddenFile(filePath, fileName, hiddenFiles)) {
      continue;
    }

    let currentPath = "";
    let i = 0;
    let depth = 0;

    while (i < segments.length) {
      const name = segments[i];
      const fullPath = (currentPath += `/${name}`);

      if (!fullPath.startsWith(rootFolder) || (hideRoot && fullPath === rootFolder)) {
        i++;
        continue;
      }

      if (i === segments.length - 1 && dirent?.type === "file") {
        fileList.push({
          kind: "file",
          id: fileList.length,
          name,
          fullPath,
          depth: depth + defaultDepth,
        });
      } else if (!folderPaths.has(fullPath)) {
        folderPaths.add(fullPath);

        fileList.push({
          kind: "folder",
          id: fileList.length,
          name,
          fullPath,
          depth: depth + defaultDepth,
        });
      }

      i++;
      depth++;
    }
  }

  return sortFileList(rootFolder, fileList, hideRoot);
}

function isHiddenFile(filePath: string, fileName: string, hiddenFiles: Array<string | RegExp>) {
  return hiddenFiles.some((pathOrRegex) => {
    if (typeof pathOrRegex === "string") {
      return fileName === pathOrRegex;
    }
    return pathOrRegex.test(filePath);
  });
}

function sortFileList(rootFolder: string, nodeList: Node[], hideRoot: boolean): Node[] {
  const nodeMap = new Map<string, Node>();
  const childrenMap = new Map<string, Node[]>();

  nodeList.sort((a, b) => compareNodes(a, b));

  for (const node of nodeList) {
    nodeMap.set(node.fullPath, node);

    const parentPath = node.fullPath.slice(0, node.fullPath.lastIndexOf("/"));

    if (parentPath !== rootFolder.slice(0, rootFolder.lastIndexOf("/"))) {
      if (!childrenMap.has(parentPath)) {
        childrenMap.set(parentPath, []);
      }
      childrenMap.get(parentPath)?.push(node);
    }
  }

  const sortedList: Node[] = [];

  const depthFirstTraversal = (path: string): void => {
    const node = nodeMap.get(path);
    if (node) sortedList.push(node);

    const children = childrenMap.get(path);
    if (children) {
      for (const child of children) {
        if (child.kind === "folder") {
          depthFirstTraversal(child.fullPath);
        } else {
          sortedList.push(child);
        }
      }
    }
  };

  if (hideRoot) {
    const rootChildren = childrenMap.get(rootFolder) || [];
    for (const child of rootChildren) {
      depthFirstTraversal(child.fullPath);
    }
  } else {
    depthFirstTraversal(rootFolder);
  }

  return sortedList;
}

function compareNodes(a: Node, b: Node): number {
  if (a.kind !== b.kind) {
    return a.kind === "folder" ? -1 : 1;
  }
  return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: "base" });
}
