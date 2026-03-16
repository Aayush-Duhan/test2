"use server";

import { promises as fs } from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export const PROJECT_ROOT = path.resolve(process.cwd(), "..", "data");
export const MAX_FILE_BYTES = 50 * 1024 * 1024;
export const MAX_READ_BYTES = 512 * 1024;
export const MAX_LIST_ENTRIES = 5000;
export const MAX_SEARCH_RESULTS = 200;
export const BINARY_CHECK_BYTES = 8192;

export const ALLOWED_EXTENSIONS = new Set([
  ".sql",
  ".ddl",
  ".btq",
  ".txt",
  ".csv",
  ".json",
  ".yaml",
  ".yml",
  ".md",
  ".html",
  ".xml",
  ".log",
  ".ini",
  ".cfg",
]);

export type ResolvedPath = {
  absPath: string;
  relativePath: string;
  stat?: import("node:fs").Stats;
};

export function normalizeUserPath(input: string): string {
  return input.replace(/\\/g, "/").trim();
}

export function isPathInside(rootAbs: string, targetAbs: string): boolean {
  const rel = path.relative(rootAbs, targetAbs);
  return rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel));
}

function isHiddenPath(relativePath: string): boolean {
  const segments = relativePath.split(path.sep).filter(Boolean);
  return segments.some((segment) => segment.startsWith("."));
}

export function isBinaryBuffer(buffer: Buffer): boolean {
  const checkLength = Math.min(buffer.length, BINARY_CHECK_BYTES);
  for (let i = 0; i < checkLength; i += 1) {
    if (buffer[i] === 0) {
      return true;
    }
  }
  return false;
}

export function sha256Buffer(buffer: Buffer): string {
  return crypto.createHash("sha256").update(buffer).digest("hex");
}

export async function resolveSafePath(
  userPath: string,
  options?: {
    mustExist?: boolean;
    allowDir?: boolean;
    allowFile?: boolean;
    allowHidden?: boolean;
    enforceExtensions?: boolean;
  }
): Promise<ResolvedPath> {
  if (!userPath) {
    throw new Error("path is required");
  }

  const {
    mustExist = false,
    allowDir = false,
    allowFile = true,
    allowHidden = false,
    enforceExtensions = true,
  } = options ?? {};

  const normalized = normalizeUserPath(userPath).replace(/^[/\\]+/, "");
  const absPath = path.resolve(PROJECT_ROOT, normalized);
  const relativePath = path.relative(PROJECT_ROOT, absPath);

  if (!isPathInside(PROJECT_ROOT, absPath)) {
    throw new Error("Path traversal not allowed");
  }

  if (!allowHidden && isHiddenPath(relativePath)) {
    throw new Error("Hidden paths are not allowed");
  }

  let stat: import("node:fs").Stats | undefined;
  try {
    stat = await fs.stat(absPath);
  } catch {
    stat = undefined;
  }

  if (mustExist && !stat) {
    throw new Error("Path not found");
  }

  if (stat) {
    if (stat.isDirectory() && !allowDir) {
      throw new Error("Not a file");
    }
    if (stat.isFile() && !allowFile) {
      throw new Error("Not a directory");
    }
  }

  if (allowFile && enforceExtensions) {
    const ext = path.extname(absPath).toLowerCase();
    if (ext && !ALLOWED_EXTENSIONS.has(ext)) {
      throw new Error("File extension not allowed");
    }
  }

  const realRoot = await fs.realpath(PROJECT_ROOT).catch(() => PROJECT_ROOT);
  if (stat) {
    const realTarget = await fs.realpath(absPath).catch(() => absPath);
    if (!isPathInside(realRoot, realTarget)) {
      throw new Error("Path traversal not allowed");
    }
  } else {
    const parent = path.dirname(absPath);
    const realParent = await fs.realpath(parent).catch(() => parent);
    if (!isPathInside(realRoot, realParent)) {
      throw new Error("Path traversal not allowed");
    }
  }

  return { absPath, relativePath, stat };
}

