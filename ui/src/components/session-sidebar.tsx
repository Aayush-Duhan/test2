"use client";

import * as React from "react";
import { useRouter, usePathname } from "next/navigation";
import { ChevronDown, ChevronRight, FolderOpen, LayoutDashboard } from "lucide-react";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
  useSidebar,
} from "@/components/ui/sidebar";
import type { SessionSummary } from "@/lib/chat-types";
import { isActive } from "@/lib/chat-helpers";

interface SessionSidebarProps {
  /** Highlight a specific session */
  selectedSessionId?: string | null;
  /** Increment this to force a reload of the session list */
  reloadKey?: number;
}

export function SessionSidebar({
  selectedSessionId = null,
  reloadKey = 0,
}: SessionSidebarProps) {
  const router = useRouter();
  const pathname = usePathname();
  const [open, setOpen] = React.useState(false);
  const { setOpen: setSidebarOpen, open: isSidebarOpen } = useSidebar();

  const hoverTimeoutRef = React.useRef<NodeJS.Timeout>(undefined);

  React.useEffect(() => {
    setSidebarOpen(false);
  }, [pathname, setSidebarOpen]);

  const isDashboard = pathname === "/";
  const isSessionsPage = pathname === "/sessions" || pathname.startsWith("/sessions/");

  const [sessions, setSessions] = React.useState<SessionSummary[]>([]);
  const [loading, setLoading] = React.useState(false);

  const loadSessions = React.useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/runs?limit=100", { cache: "no-store" });
      if (!res.ok) { setSessions([]); return; }
      const payload = await res.json();
      const raw = Array.isArray(payload?.runs)
        ? (payload.runs as SessionSummary[])
        : [];
      raw.sort((a, b) => {
        const aa = isActive(a.status) ? 0 : 1;
        const bb = isActive(b.status) ? 0 : 1;
        if (aa !== bb) return aa - bb;
        return new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime();
      });
      setSessions(raw);
    } catch {
      setSessions([]);
    } finally {
      setLoading(false);
    }
  }, []);

  /* Load on mount + whenever the parent bumps reloadKey */
  React.useEffect(() => {
    void loadSessions();
  }, [loadSessions, reloadKey]);

  return (
    <Sidebar
      collapsible="icon"
      className="p-0 lg:w-64"
      onMouseEnter={() => {
        if (hoverTimeoutRef.current) clearTimeout(hoverTimeoutRef.current);
        setSidebarOpen(true);
      }}
      onMouseLeave={() => {
        hoverTimeoutRef.current = setTimeout(() => {
          setSidebarOpen(false);
        }, 150);
      }}
    >
      <SidebarHeader className="p-2">
        {/* Dashboard link */}
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              onClick={() => router.push("/")}
              isActive={isDashboard}
              className="h-10 text-white hover:text-white group-data-[collapsible=icon]:justify-center"
            >
              <LayoutDashboard className="size-4 text-white" />
              <span className="font-medium text-white group-data-[collapsible=icon]:hidden">
                Dashboard
              </span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>

        {/* Sessions link + toggle */}
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              onClick={() => router.push("/sessions")}
              isActive={isSessionsPage && !isDashboard}
              className="h-10 text-white hover:text-white group-data-[collapsible=icon]:justify-center"
            >
              <FolderOpen className="size-4 text-white" />
              <span className="font-medium text-white group-data-[collapsible=icon]:hidden">
                Sessions
              </span>
              {/* Separate toggle chevron - stops propagation so it doesn't trigger navigation */}
              <ChevronRight
                onClick={(e) => {
                  e.stopPropagation();
                  setOpen((prev) => !prev);
                }}
                className={`ml-auto size-4 cursor-pointer text-white/60 transition-transform hover:text-white group-data-[collapsible=icon]:hidden ${open ? "rotate-90" : ""}`}
              />
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent className="min-h-0 px-0">
        {open && (
          <SidebarGroup className="p-0">
            <SidebarGroupContent>
              <SidebarMenuSub className="scrollbar-dark max-h-64 overflow-y-auto">
                {loading && (
                  <SidebarMenuSubItem>
                    <p className="px-2 py-1 text-xs text-white/60">
                      Loading sessions...
                    </p>
                  </SidebarMenuSubItem>
                )}
                {sessions.map((session) => {
                  const active = session.runId === selectedSessionId;
                  return (
                    <SidebarMenuSubItem key={session.runId}>
                      <SidebarMenuSubButton
                        asChild
                        isActive={active}
                        className="text-white hover:text-white data-[active=true]:text-white"
                      >
                        <button
                          type="button"
                          onClick={() => router.push(`/sessions/${session.runId}`)}
                          className="w-full text-white"
                        >
                          <span>
                            {session.projectName}
                          </span>
                        </button>
                      </SidebarMenuSubButton>
                    </SidebarMenuSubItem>
                  );
                })}
              </SidebarMenuSub>
            </SidebarGroupContent>
          </SidebarGroup>
        )}
      </SidebarContent>
    </Sidebar>
  );
}
