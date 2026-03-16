/**
 * GET /api/fs/read?path=...&maxBytes=...
 * Reads file content from the local filesystem with guardrails.
 * Returns: { content, isBinary, truncated, sizeBytes, sha256? }
 */
import { NextRequest, NextResponse } from "next/server";
import { promises as fs } from "node:fs";
import {
  MAX_FILE_BYTES,
  MAX_READ_BYTES,
  isBinaryBuffer,
  resolveSafePath,
  sha256Buffer,
} from "@/lib/fs-guard";

export const runtime = "nodejs";

export async function GET(request: NextRequest) {
  try {
    const filePath = request.nextUrl.searchParams.get("path");
    const maxBytesParam = request.nextUrl.searchParams.get("maxBytes");

    if (!filePath) {
      return NextResponse.json(
        { error: "path query parameter is required" },
        { status: 400 },
      );
    }

    const { absPath, stat } = await resolveSafePath(filePath, {
      mustExist: true,
      allowDir: false,
      allowFile: true,
    });

    if (!stat?.isFile()) {
      return NextResponse.json({ error: "Not a file" }, { status: 400 });
    }

    if (stat.size > MAX_FILE_BYTES) {
      return NextResponse.json(
        { error: "File exceeds max size", sizeBytes: stat.size },
        { status: 413 },
      );
    }

    const requestedMax = maxBytesParam ? Number(maxBytesParam) : undefined;
    const effectiveMax =
      requestedMax && Number.isFinite(requestedMax)
        ? Math.min(Math.max(1, requestedMax), MAX_READ_BYTES)
        : MAX_READ_BYTES;

    let buffer: Buffer;
    let truncated = false;
    let sha256: string | undefined;

    if (effectiveMax >= stat.size) {
      buffer = await fs.readFile(absPath);
      sha256 = sha256Buffer(buffer);
    } else {
      const handle = await fs.open(absPath, "r");
      try {
        buffer = Buffer.alloc(effectiveMax);
        const { bytesRead } = await handle.read(buffer, 0, effectiveMax, 0);
        buffer = buffer.subarray(0, bytesRead);
        truncated = stat.size > bytesRead;
      } finally {
        await handle.close();
      }
    }

    const isBinary = isBinaryBuffer(buffer);

    return NextResponse.json({
      content: isBinary ? "" : buffer.toString("utf-8"),
      isBinary,
      truncated,
      sizeBytes: stat.size,
      sha256,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const status = (error as NodeJS.ErrnoException)?.code === "ENOENT" ? 404 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}
