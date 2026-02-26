"use client";

import Link from "next/link";
import { Header } from "@/components/header";
import { SidebarProvider, SidebarInset } from "@/components/ui/sidebar";
import { SessionSidebar } from "@/components/session-sidebar";

export default function MigrationToolkitPage() {
  return (
    <div
      className="flex h-screen flex-col overflow-hidden bg-[#1a1a1a]"
      style={{ ["--header-h" as string]: "48px" }}
    >
      <Header />

      <SidebarProvider className="sidebar-offset min-h-0 flex-1">
        <div className="flex min-h-0 w-full flex-1">
          <SessionSidebar />

          <SidebarInset className="flex min-h-0 flex-1 flex-col overflow-hidden bg-[#07080c]">
            <div className="relative flex min-h-0 flex-1 items-center justify-center overflow-hidden px-4 pb-10 pt-8 sm:px-6">
              <div className="pointer-events-none absolute inset-0 z-0 bg-[url('/migration-toolkit-bg.png')] bg-cover bg-center opacity-95 saturate-[1.2]" />
              <div className="pointer-events-none absolute inset-0 z-0 bg-[linear-gradient(90deg,rgba(7,8,12,0.34)_0%,rgba(7,8,12,0.62)_42%,rgba(7,8,12,0.9)_100%)]" />
              <div className="pointer-events-none absolute inset-0 z-0 bg-[radial-gradient(circle_at_24%_28%,rgba(255,132,32,0.15),transparent_42%),radial-gradient(circle_at_72%_16%,rgba(20,136,252,0.22),transparent_45%),radial-gradient(circle_at_86%_72%,rgba(255,255,255,0.08),transparent_30%)]" />

              <div className="relative z-10 flex w-full max-w-[860px] flex-col items-center text-center">
                <h1 className="mt-7 text-4xl font-bold leading-[1.05] tracking-tight text-white sm:text-5xl">
                  Migration Toolkit
                </h1>
                <p className="mt-3 max-w-[760px] text-base font-semibold text-[#8a8a8f] sm:text-lg">
                  Set up a guided migration session to convert source SQL into Snowflake-ready output using the
                  built-in toolkit.
                </p>

                <div className="mt-4 flex flex-wrap items-center justify-center gap-2 text-xs text-[#b3b3b8] sm:text-sm">
                  <span className="rounded-full border border-white/10 bg-white/[0.04] px-3 py-1.5">
                    Choose source database and content type
                  </span>
                  <span className="rounded-full border border-white/10 bg-white/[0.04] px-3 py-1.5">
                    Upload source SQL files
                  </span>
                  <span className="rounded-full border border-white/10 bg-white/[0.04] px-3 py-1.5">
                    Provide Snowflake connection details
                  </span>
                </div>

                <Link
                  href="/sessions"
                  className="mt-7 rounded-full bg-[#1488fc] px-5 py-2 text-sm font-medium text-white transition-colors hover:bg-[#1a94ff]"
                >
                  Start session
                </Link>
              </div>
            </div>
          </SidebarInset>
        </div>
      </SidebarProvider>
    </div>
  );
}
