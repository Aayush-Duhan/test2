import { NextResponse } from "next/server";
import crypto from "crypto";
import { ensureStorage, saveProject } from "@/lib/storage";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const body = await request.json().catch(() => null);
  if (body?.name && typeof body.name !== "string") {
    return NextResponse.json({ error: "Project name must be a string when provided" }, { status: 400 });
  }

  await ensureStorage();
  const projectId = crypto.randomUUID();
  const projectName = typeof body?.name === "string" && body.name.trim().length > 0 ? body.name.trim() : projectId;
  const record = {
    projectId,
    name: projectName,
    sourceLanguage: typeof body?.sourceLanguage === "string" ? body.sourceLanguage : undefined,
    createdAt: new Date().toISOString(),
  };
  await saveProject(record);

  return NextResponse.json({ projectId, name: projectName });
}
