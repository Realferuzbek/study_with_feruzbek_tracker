import { NextResponse } from "next/server";

const UNAUTHORIZED = NextResponse.json({ error: "Unauthorized" }, { status: 401 });

function isAuthorized(req: Request, expected: string): boolean {
  if (!expected) return false;
  const headerSecret = req.headers.get("x-cron-secret");
  if (headerSecret && headerSecret === expected) {
    return true;
  }
  const auth = req.headers.get("authorization");
  if (auth?.startsWith("Bearer ")) {
    const token = auth.slice("Bearer ".length).trim();
    if (token && token === expected) {
      return true;
    }
  }
  const url = new URL(req.url);
  const qs = url.searchParams.get("secret");
  return Boolean(qs && qs === expected);
}

export async function GET(req: Request) {
  const cronSecret = process.env.CRON_SECRET ?? "";
  if (!cronSecret) {
    return NextResponse.json({ error: "CRON_SECRET not configured." }, { status: 500 });
  }
  if (!isAuthorized(req, cronSecret)) {
    return UNAUTHORIZED;
  }
  const sessionVersion = Number(process.env.SESSION_VERSION ?? "1");
  return NextResponse.json({
    session_version: sessionVersion,
    updated_at: new Date().toISOString(),
  });
}
