/**
 * POST /api/fs/write
 * Writes file content to the local filesystem.
 * Body: { filePath: string, content: string, expectedHash?: string }
 */
import { NextRequest, NextResponse } from "next/server";
import { promises as fs } from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import {
  MAX_FILE_BYTES,
  resolveSafePath,
  sha256Buffer,
} from "@/lib/fs-guard";

export const runtime = "nodejs";

async function writeAtomic(targetPath: string, content: string) {
  const dir = path.dirname(targetPath);
  const tmpName = `.tmp-${Date.now()}-${crypto.randomUUID()}.tmp`;
  const tmpPath = path.join(dir, tmpName);
  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(tmpPath, content, "utf-8");
  await fs.rename(tmpPath, targetPath);
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { filePath, content, expectedHash } = body as {
      filePath: string;
      content: string;
      expectedHash?: string;
    };

    if (!filePath || typeof content !== "string") {
      return NextResponse.json(
        { error: "filePath and content are required" },
        { status: 400 },
      );
    }

    const byteSize = Buffer.byteLength(content, "utf-8");
    if (byteSize > MAX_FILE_BYTES) {
      return NextResponse.json(
        { error: "Content exceeds max size", sizeBytes: byteSize },
        { status: 413 },
      );
    }

    const { absPath, stat } = await resolveSafePath(filePath, {
      mustExist: false,
      allowDir: false,
      allowFile: true,
      enforceExtensions: true,
    });

    if (stat?.isDirectory()) {
      return NextResponse.json({ error: "Not a file" }, { status: 400 });
    }

    if (expectedHash && stat?.isFile()) {
      const existing = await fs.readFile(absPath);
      const currentHash = sha256Buffer(existing);
      if (expectedHash !== currentHash) {
        return NextResponse.json(
          { error: "File hash does not match expectedHash", currentHash },
          { status: 409 },
        );
      }
    }

    await writeAtomic(absPath, content);

    return NextResponse.json({
      success: true,
      path: absPath,
      bytesWritten: byteSize,
      newHash: sha256Buffer(Buffer.from(content, "utf-8")),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
