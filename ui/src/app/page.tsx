"use client";

import type { ComponentType } from "react";
import Link from "next/link";
import { Header } from "@/components/header";
import { SidebarProvider, SidebarInset } from "@/components/ui/sidebar";
import { SessionSidebar } from "@/components/session-sidebar";
import {
  BarChart3,
  CheckCircle2,
  FileSearch,
  GitMerge,
  Layers,
  PlayCircle,
  Rocket,
  ScanSearch,
  Settings,
  ShieldCheck,
  Wrench,
} from "lucide-react";

interface DashboardAction {
  label: string;
  href?: string;
  icon: ComponentType<{ className?: string }>;
  enabled?: boolean;
}

interface DashboardModule {
  id: string;
  label: string;
  icon: ComponentType<{ className?: string }>;
  angleDeg: number;
  actions: DashboardAction[];
}

const MODULE_ORBIT_RADIUS = 160;
const ACTION_ORBIT_RADIUS = 52;
const ACTION_OFFSET_X = 13;
const ACTION_OFFSET_Y = 14;

const modules: DashboardModule[] = [
  {
    id: "maxx",
    label: "Release",
    icon: Rocket,
    angleDeg: -90,
    actions: [
      { label: "Release Management", icon: Rocket },
      { label: "App Compare", icon: ScanSearch },
    ],
  },
  {
    id: "ella",
    label: "Definition",
    icon: FileSearch,
    angleDeg: -30,
    actions: [
      { label: "Summarisation", icon: FileSearch },
      { label: "STM Generation", icon: Layers },
      { label: "Data Lineage", icon: GitMerge },
    ],
  },
  {
    id: "alex",
    label: "Build",
    icon: Wrench,
    angleDeg: 30,
    actions: [
      { label: "Migration Toolkit", href: "/migration-toolkit", icon: Wrench, enabled: true },
      { label: "Talend Migration", icon: GitMerge },
    ],
  },
  {
    id: "luna",
    label: "Review",
    icon: ScanSearch,
    angleDeg: 90,
    actions: [
      { label: "Code Review", icon: ScanSearch },
      { label: "Code Execute", icon: PlayCircle },
      { label: "Code Reconciliation", icon: CheckCircle2 },
    ],
  },
  {
    id: "finn",
    label: "Test",
    icon: ShieldCheck,
    angleDeg: 150,
    actions: [
      { label: "Execute Test Cases", icon: PlayCircle },
      { label: "Testing Metrics", icon: BarChart3 },
      { label: "Test Case Generation", icon: Settings },
    ],
  },
  {
    id: "ilsa",
    label: "Integrate",
    icon: GitMerge,
    angleDeg: 210,
    actions: [
      { label: "GIT Integration", icon: GitMerge },
      { label: "ADO Integration", icon: Layers },
    ],
  },
];

function getActionAngles(count: number) {
  return Array.from({ length: count }, (_, index) => -90 + index * 90);
}

export default function DashboardPage() {
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
            <div className="relative flex min-h-0 flex-1 overflow-hidden">
              <div className="pointer-events-none absolute inset-0 bg-[url('/dashboard-bg.png')] bg-cover bg-left opacity-45" />
              <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(90deg,rgba(7,8,12,0.42)_0%,rgba(7,8,12,0.74)_36%,rgba(7,8,12,0.93)_100%)]" />
              <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_78%_34%,rgba(20,136,252,0.18),transparent_48%),radial-gradient(circle_at_92%_65%,rgba(255,255,255,0.08),transparent_34%)]" />

              <div className="relative flex h-full w-full items-center justify-center p-4 sm:p-6 md:justify-end md:pr-16 lg:pr-28">
                <div className="relative h-[470px] w-[470px] max-h-[82%] max-w-[82%]">
                  <div className="absolute left-1/2 top-1/2 h-[156px] w-[156px] -translate-x-1/2 -translate-y-1/2 rounded-full border border-[#1488fc]/70 bg-[radial-gradient(circle_at_35%_30%,#1488fc,#10345e_62%,#0c1729)] shadow-[0_0_22px_rgba(20,136,252,0.45),0_0_70px_rgba(20,136,252,0.2)]">
                    <div className="flex h-full w-full flex-col items-center justify-center text-center">
                      <h1 className="text-2xl font-semibold tracking-[0.2em] text-white">ETHAN</h1>
                      <p className="mt-1 px-5 text-[8px] uppercase tracking-[0.2em] text-[#d4e8ff]">
                        Efficient Thinking Autonomous Network
                      </p>
                    </div>
                  </div>

                  {modules.map((module) => {
                    const angleRad = (module.angleDeg * Math.PI) / 180;
                    const x = Math.cos(angleRad) * MODULE_ORBIT_RADIUS;
                    const y = Math.sin(angleRad) * MODULE_ORBIT_RADIUS;
                    const actionAngles = getActionAngles(module.actions.length);

                    return (
                      <div
                        key={module.id}
                        className="group absolute left-1/2 top-1/2"
                        style={{
                          transform: `translate(calc(-50% + ${x}px), calc(-50% + ${y}px))`,
                        }}
                      >
                        <div className="absolute left-1/2 top-1/2 h-[142px] w-[142px] -translate-x-1/2 -translate-y-1/2 rounded-full border border-dashed border-white/20" />

                        {module.actions.map((action, index) => {
                          const actionAngle = actionAngles[index];
                          const actionRad = (actionAngle * Math.PI) / 180;
                          const ax = Math.cos(actionRad) * ACTION_ORBIT_RADIUS;
                          const ay = Math.sin(actionRad) * ACTION_ORBIT_RADIUS;
                          const ActionIcon = action.icon;
                          const enabled = Boolean(action.enabled);
                          const itemClass = enabled
                            ? "border-[#1a94ff] bg-[#1e1e22] text-[#b6daff] shadow-[0_0_12px_rgba(20,136,252,0.5)] hover:border-[#74bdff] hover:text-white"
                            : "border-white/20 bg-[#1e1e22] text-white/65 shadow-[0_0_10px_rgba(20,136,252,0.25)] hover:text-white/85";

                          const sharedProps = {
                            className: `absolute left-1/2 top-1/2 z-20 flex h-[30px] w-[30px] -translate-x-1/2 -translate-y-1/2 items-center justify-center rounded-full border transition ${itemClass}`,
                            style: {
                              transform: `translate(calc(-50% + ${ax + ACTION_OFFSET_X}px), calc(-50% + ${ay + ACTION_OFFSET_Y}px))`,
                            },
                            title: enabled ? `${action.label} (Available)` : `${action.label} (Coming soon)`,
                          };

                          if (enabled && action.href) {
                            return (
                              <Link key={action.label} href={action.href} {...sharedProps}>
                                <ActionIcon className="h-3.5 w-3.5" />
                              </Link>
                            );
                          }

                          return (
                            <button key={action.label} type="button" disabled {...sharedProps}>
                              <ActionIcon className="h-3.5 w-3.5" />
                            </button>
                          );
                        })}

                        <div className="relative z-10 flex h-[68px] w-[68px] items-center justify-center rounded-full border border-white/20 bg-[linear-gradient(135deg,#1e1e22,#171a22)] shadow-[0_0_14px_rgba(20,136,252,0.25)] transition-transform group-hover:scale-105">
                          <div className="flex flex-col items-center gap-1">
                            <module.icon className="h-4 w-4 text-[#9fceff]" />
                            <span className="text-[10px] font-medium tracking-wide text-white">{module.label}</span>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          </SidebarInset>
        </div>
      </SidebarProvider>
    </div>
  );
}
