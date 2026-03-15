import { NextResponse } from "next/server";

export const runtime = "nodejs";

export async function POST() {
  return NextResponse.json(
    { error: "GitHub import has moved to /api/codehub/tree." },
    { status: 410 }
  );
}
