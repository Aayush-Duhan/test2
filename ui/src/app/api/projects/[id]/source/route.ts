import { NextResponse } from "next/server";
import crypto from "crypto";
import path from "path";
import { promises as fs } from "fs";
import { ensureStorage, getUploadDir, saveSource } from "@/lib/storage";

export const runtime = "nodejs";

function sanitizeFilename(name: string) {
  return name.replace(/[^a-zA-Z0-9._-]/g, "_");
}

export async function POST(
  request: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  const formData = await request.formData();
  const file = formData.get("file");
  if (!file || !(file instanceof File)) {
    return NextResponse.json({ error: "SQL file is required" }, { status: 400 });
  }

  await ensureStorage();
  const { id: projectId } = await params;
  const sourceId = crypto.randomUUID();
  const safeName = sanitizeFilename(file.name || "source.sql");
  const uploadDir = await getUploadDir(projectId);
  const targetPath = path.join(uploadDir, `${sourceId}-${safeName}`);

  const buffer = Buffer.from(await file.arrayBuffer());
  await fs.writeFile(targetPath, buffer);

  await saveSource({
    sourceId,
    projectId,
    filename: safeName,
    filepath: targetPath,
    createdAt: new Date().toISOString(),
  });

  return NextResponse.json({ sourceId, filename: safeName });
}
