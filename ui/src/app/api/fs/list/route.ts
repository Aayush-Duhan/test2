/**
 * GET /api/fs/list?path=...&depth=1&pattern=...&includeFiles=1&includeDirs=1&includeHidden=0
 * Lists directory entries under the local filesystem with guardrails.
 */
import { NextRequest, NextResponse } from "next/server";
import { promises as fs } from "node:fs";
import path from "node:path";
import {
  MAX_LIST_ENTRIES,
  PROJECT_ROOT,
  resolveSafePath,
} from "@/lib/fs-guard";

export const runtime = "nodejs";

function toPosix(input: string) {
  return input.split(path.sep).join("/");
}

function parseBoolean(input: string | null, fallback: boolean) {
  if (input === null) return fallback;
  return input === "1" || input.toLowerCase() === "true";
}

function globToRegex(pattern: string): RegExp | null {
  if (!pattern) return null;
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  const regex = "^" + escaped.replace(/\*/g, ".*").replace(/\?/g, ".") + "$";
  return new RegExp(regex, "i");
}

export async function GET(request: NextRequest) {
  try {
    const dirPath = request.nextUrl.searchParams.get("path") ?? "";
    const depthParam = request.nextUrl.searchParams.get("depth");
    const pattern = request.nextUrl.searchParams.get("pattern") ?? "";
    const includeFiles = parseBoolean(request.nextUrl.searchParams.get("includeFiles"), true);
    const includeDirs = parseBoolean(request.nextUrl.searchParams.get("includeDirs"), true);
    const includeHidden = parseBoolean(request.nextUrl.searchParams.get("includeHidden"), false);

    const depth = depthParam ? Math.max(0, Number(depthParam)) : 1;
    const matcher = globToRegex(pattern);

    const { absPath } = await resolveSafePath(dirPath || ".", {
      mustExist: true,
      allowDir: true,
      allowFile: false,
      allowHidden: includeHidden,
    });

    const entries: Array<{ path: string; type: "file" | "dir"; sizeBytes?: number }> = [];
    let truncated = false;

    const baseDepth = absPath.split(path.sep).length;
    const queue = [absPath];

    while (queue.length > 0) {
      const current = queue.shift();
      if (!current) break;

      const currentDepth = current.split(path.sep).length - baseDepth;
      if (currentDepth > depth) {
        continue;
      }

      const dirents = await fs.readdir(current, { withFileTypes: true });
      for (const dirent of dirents) {
        if (!includeHidden && dirent.name.startsWith(".")) {
          continue;
        }

        if (dirent.isSymbolicLink()) {
          continue;
        }

        if (matcher && !matcher.test(dirent.name)) {
          continue;
        }

        const full = path.join(current, dirent.name);
        const rel = toPosix(path.relative(PROJECT_ROOT, full));

        if (dirent.isDirectory()) {
          if (includeDirs) {
            entries.push({ path: rel, type: "dir" });
          }
          if (currentDepth < depth) {
            queue.push(full);
          }
        } else if (dirent.isFile()) {
          if (includeFiles) {
            const stat = await fs.stat(full).catch(() => null);
            entries.push({ path: rel, type: "file", sizeBytes: stat?.size ?? 0 });
          }
        }

        if (entries.length >= MAX_LIST_ENTRIES) {
          truncated = true;
          queue.length = 0;
          break;
        }
      }
    }

    return NextResponse.json({ entries, truncated });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
