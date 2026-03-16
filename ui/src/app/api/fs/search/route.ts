/**
 * GET /api/fs/search?path=...&query=...&regex=0&caseSensitive=0&maxResults=200
 * Searches within a file with guardrails.
 */
import { NextRequest, NextResponse } from "next/server";
import { promises as fs } from "node:fs";
import {
  BINARY_CHECK_BYTES,
  MAX_FILE_BYTES,
  MAX_SEARCH_RESULTS,
  isBinaryBuffer,
  resolveSafePath,
} from "@/lib/fs-guard";

export const runtime = "nodejs";

export async function GET(request: NextRequest) {
  try {
    const filePath = request.nextUrl.searchParams.get("path");
    const query = request.nextUrl.searchParams.get("query") ?? "";
    const regexParam = request.nextUrl.searchParams.get("regex");
    const caseParam = request.nextUrl.searchParams.get("caseSensitive");
    const maxResultsParam = request.nextUrl.searchParams.get("maxResults");

    if (!filePath) {
      return NextResponse.json({ error: "path query parameter is required" }, { status: 400 });
    }
    if (!query) {
      return NextResponse.json({ error: "query is required" }, { status: 400 });
    }

    const regex = regexParam === "1" || regexParam?.toLowerCase() === "true";
    const caseSensitive = caseParam === "1" || caseParam?.toLowerCase() === "true";
    const maxResultsRaw = maxResultsParam ? Number(maxResultsParam) : undefined;
    const maxResults =
      maxResultsRaw && Number.isFinite(maxResultsRaw)
        ? Math.min(Math.max(1, maxResultsRaw), MAX_SEARCH_RESULTS)
        : MAX_SEARCH_RESULTS;

    const { absPath, stat } = await resolveSafePath(filePath, {
      mustExist: true,
      allowDir: false,
      allowFile: true,
    });

    if (!stat?.isFile()) {
      return NextResponse.json({ error: "Not a file" }, { status: 400 });
    }

    if (stat.size > MAX_FILE_BYTES) {
      return NextResponse.json({ error: "File exceeds max size", sizeBytes: stat.size }, { status: 413 });
    }

    const handle = await fs.open(absPath, "r");
    try {
      const head = Buffer.alloc(Math.min(BINARY_CHECK_BYTES, stat.size));
      await handle.read(head, 0, head.length, 0);
      if (isBinaryBuffer(head)) {
        return NextResponse.json({ error: "Binary files are not allowed", matches: [] }, { status: 415 });
      }
    } finally {
      await handle.close();
    }

    const text = await fs.readFile(absPath, "utf-8");
    const lines = text.split(/\r?\n/);
    const matches: Array<{ line: number; text: string }> = [];
    let truncated = false;

    let pattern: RegExp | null = null;
    if (regex) {
      try {
        pattern = new RegExp(query, caseSensitive ? "" : "i");
      } catch {
        return NextResponse.json({ error: "Invalid regex", matches: [] }, { status: 400 });
      }
    }

    const needle = caseSensitive ? query : query.toLowerCase();

    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      const haystack = caseSensitive ? line : line.toLowerCase();
      const found = pattern ? pattern.test(line) : haystack.includes(needle);
      if (!found) continue;
      const trimmed = line.length > 500 ? `${line.slice(0, 500)}...` : line;
      matches.push({ line: i + 1, text: trimmed });
      if (matches.length >= maxResults) {
        truncated = true;
        break;
      }
    }

    return NextResponse.json({ matches, truncated });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
