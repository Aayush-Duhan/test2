"use client";

import { useMemo, useState, type KeyboardEvent } from "react";
import { useRouter } from "next/navigation";
import { Header } from "@/components/header";
import { SidebarProvider, SidebarInset } from "@/components/ui/sidebar";
import { SessionSidebar } from "@/components/session-sidebar";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import {
  Database,
  FlaskConical,
  Loader2,
  type LucideIcon,
  Snowflake,
} from "lucide-react";

interface MigrationModule {
  id: string;
  title: string;
  description: string;
  helper: string;
  tooltip: string;
  ctaLabel?: string;
  route?: string;
  enabled: boolean;
  icon: LucideIcon;
}

const modules: MigrationModule[] = [
  {
    id: "snowflake",
    title: "Snowflake Migration",
    description: "Convert SQL scripts and data workflows to Snowflake.",
    helper: "Available now",
    tooltip: "Snowflake migration is available.",
    ctaLabel: "Start Migration",
    route: "/migration/snowflake",
    enabled: true,
    icon: Snowflake,
  },
  {
    id: "test",
    title: "Test Environment",
    description: "Validate migrations and run verification tests.",
    helper: "Coming soon",
    tooltip: "Testing tools will be available in a future release.",
    enabled: false,
    icon: FlaskConical,
  },
  {
    id: "databricks",
    title: "Databricks Migration",
    description: "Convert SQL workloads to Databricks SQL.",
    helper: "Coming soon",
    tooltip: "Databricks migration support is under development.",
    enabled: false,
    icon: Database,
  },
];

interface ModuleCardProps {
  module: MigrationModule;
  isLoading: boolean;
  className?: string;
  onActivate: () => void;
}

function ModuleCard({ module, isLoading, className, onActivate }: ModuleCardProps) {
  const Icon = module.icon;
  const isDisabled = !module.enabled;

  const commonClassName = cn(
    "group relative flex w-full flex-col gap-4 rounded-2xl border p-5 text-left transition-all duration-200 ease-out focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-400/70",
    module.enabled
      ? "border-blue-500/85 bg-[#101a2a]/85 text-white shadow-lg shadow-blue-500/20 hover:border-blue-400 hover:shadow-blue-500/30"
      : "border-dashed border-white/25 bg-[#171922]/70 text-white/85 opacity-60",
    className,
  );

  const cardContent = (
    <button
      type="button"
      aria-disabled={isDisabled}
      title={module.tooltip}
      onClick={(event) => {
        if (isDisabled) {
          event.preventDefault();
          return;
        }
        onActivate();
      }}
      onKeyDown={(event: KeyboardEvent<HTMLButtonElement>) => {
        if (!isDisabled) {
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
        }
      }}
      className={commonClassName}
    >
      {isLoading ? (
        <span className="pointer-events-none absolute inset-0 overflow-hidden rounded-2xl">
          <span className="absolute inset-0 bg-[linear-gradient(110deg,transparent_20%,rgba(255,255,255,0.18)_50%,transparent_80%)] [background-size:200%_100%] animate-[shimmer_1.1s_linear_infinite]" />
        </span>
      ) : null}

      <div className="relative flex items-center gap-3">
        <span
          className={cn(
            "inline-flex h-10 w-10 items-center justify-center rounded-xl border",
            module.enabled ? "border-blue-400/50 bg-blue-500/20" : "border-white/20 bg-white/5",
          )}
        >
          <Icon className="h-5 w-5" />
        </span>
        <h2 className="text-md font-medium text-white">{module.title}</h2>
      </div>

      <p className="relative text-sm text-[#cfd8e5]">{module.description}</p>

      <div className="relative mt-1 flex items-center justify-between gap-3">
        <span className="text-xs font-medium tracking-wide text-[#aab6c6]">{module.helper}</span>

        {module.enabled ? (
          <span className="inline-flex items-center gap-2 rounded-full bg-blue-500 px-3 py-1.5 text-xs font-semibold text-white">
            {module.ctaLabel}
            {isLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden="true" /> : null}
          </span>
        ) : null}
      </div>
    </button>
  );

  if (module.enabled) {
    return cardContent;
  }

  return (
    <Tooltip>
      <TooltipTrigger asChild>{cardContent}</TooltipTrigger>
      <TooltipContent side="top" sideOffset={8}>
        {module.tooltip}
      </TooltipContent>
    </Tooltip>
  );
}

export default function DashboardPage() {
  const router = useRouter();
  const [isLaunchingSnowflake, setIsLaunchingSnowflake] = useState(false);

  const snowflakeModule = modules[0];
  const secondaryModules = useMemo(() => modules.slice(1), []);

  const handleSnowflakeActivate = () => {
    if (!snowflakeModule.route || isLaunchingSnowflake) {
      return;
    }

    setIsLaunchingSnowflake(true);
    window.setTimeout(() => {
      router.push(snowflakeModule.route!);
    }, 220);
  };

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
              <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(120deg,rgba(5,8,15,0.94)_0%,rgba(8,12,20,0.9)_32%,rgba(10,16,30,0.9)_100%)]" />
              <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_20%_18%,rgba(20,136,252,0.22),transparent_45%),radial-gradient(circle_at_86%_74%,rgba(255,255,255,0.06),transparent_38%)]" />

              <div className="relative flex h-full w-full items-center justify-center px-4 py-8 sm:px-6 md:px-10">
                <TooltipProvider>
                  <div className="w-full max-w-4xl">
                    <div className="mx-auto max-w-2xl text-center">
                      <h1 className="text-2xl font-semibold text-white">Migration Workspace</h1>
                      <p className="mt-2 text-sm text-[#b4c0cf]">
                        Start a migration workflow or explore upcoming capabilities.
                      </p>
                    </div>

                    <div className="mx-auto mt-8 max-w-3xl border-t border-white/12 pt-5 text-center">
                      <p className="text-sm uppercase tracking-[0.2em] text-[#96a6bb]">Data Migration Platform</p>
                    </div>

                    <div className="mx-auto mt-5 grid max-w-3xl grid-cols-1 gap-4 sm:grid-cols-2">
                      <ModuleCard
                        module={snowflakeModule}
                        isLoading={isLaunchingSnowflake}
                        onActivate={handleSnowflakeActivate}
                        className="sm:col-span-2 sm:mx-auto sm:w-[min(100%,30rem)]"
                      />

                      {secondaryModules.map((module) => (
                        <ModuleCard
                          key={module.id}
                          module={module}
                          isLoading={false}
                          onActivate={() => {}}
                          className="sm:min-h-[210px]"
                        />
                      ))}
                    </div>
                  </div>
                </TooltipProvider>
                </div>
              </div>
          </SidebarInset>
        </div>
      </SidebarProvider>
    </div>
  );
}
