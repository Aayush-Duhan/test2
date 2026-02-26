import { NextResponse } from "next/server";
import crypto from "crypto";
import path from "path";
import { promises as fs } from "fs";
import { ensureStorage, getUploadDir, saveSchema } from "@/lib/storage";

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
    return NextResponse.json({ error: "Schema file is required" }, { status: 400 });
  }

  await ensureStorage();
  const { id: projectId } = await params;
  const schemaId = crypto.randomUUID();
  const safeName = sanitizeFilename(file.name || "schema.csv");
  const uploadDir = await getUploadDir(projectId);
  const targetPath = path.join(uploadDir, `${schemaId}-${safeName}`);

  const buffer = Buffer.from(await file.arrayBuffer());
  await fs.writeFile(targetPath, buffer);

  await saveSchema({
    schemaId,
    projectId,
    filename: safeName,
    filepath: targetPath,
    createdAt: new Date().toISOString(),
  });

  return NextResponse.json({ schemaId, filename: safeName });
}
