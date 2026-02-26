"use client";

import * as React from "react";
import { CheckCircle2, Circle, CircleAlert, CircleDotDashed, CircleX, ChevronDown } from "lucide-react";

export interface Subtask {
  id: string;
  title: string;
  description: string;
  status: string;
  priority: string;
  tools?: string[];
}

export interface Task {
  id: string;
  title: string;
  description: string;
  status: string;
  priority: string;
  level: number;
  dependencies: string[];
  subtasks: Subtask[];
}

type PlanProps = {
  tasks?: Task[];
  readOnly?: boolean;
};

const STATUS_LABEL: Record<string, string> = {
  completed: "Completed",
  "in-progress": "Running",
  pending: "Pending",
  failed: "Failed",
  skipped: "Skipped",
  "need-help": "Needs input",
  paused: "Paused",
};

function statusIcon(status: string) {
  if (status === "completed") return <CheckCircle2 className="h-4 w-4 text-emerald-400" />;
  if (status === "in-progress") return <CircleDotDashed className="h-4 w-4 text-sky-400" />;
  if (status === "failed") return <CircleX className="h-4 w-4 text-red-400" />;
  if (status === "skipped") return <Circle className="h-4 w-4 text-slate-400" />;
  if (status === "need-help" || status === "paused") return <CircleAlert className="h-4 w-4 text-amber-400" />;
  return <Circle className="h-4 w-4 text-white/40" />;
}

function statusBadgeClass(status: string): string {
  if (status === "completed") return "bg-emerald-500/15 text-emerald-200 border-emerald-400/30";
  if (status === "in-progress") return "bg-sky-500/15 text-sky-200 border-sky-400/30";
  if (status === "failed") return "bg-red-500/15 text-red-200 border-red-400/30";
  if (status === "skipped") return "bg-slate-500/15 text-slate-200 border-slate-400/30";
  if (status === "need-help" || status === "paused") return "bg-amber-500/15 text-amber-200 border-amber-400/30";
  return "bg-white/5 text-white/65 border-white/10";
}

export default function AgentPlan({ tasks = [], readOnly = true }: PlanProps) {
  const [collapsed, setCollapsed] = React.useState(false);

  const workflow = tasks[0];
  const steps = workflow?.subtasks ?? [];
  const hasSteps = steps.length > 0;

  return (
    <div className="h-auto overflow-hidden rounded-lg border border-white/10 bg-[#1f1f1f] shadow">
      <div className="flex items-center justify-between border-b border-white/10 px-4 py-3">
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-white/55">Agent Plan</p>
          <p className="mt-1 text-sm font-semibold text-white/90">
            {workflow?.title ?? "Migration Workflow"}
          </p>
        </div>
        <button
          type="button"
          onClick={() => setCollapsed((v) => !v)}
          className="rounded-md p-1 text-white/70 transition hover:bg-white/10 hover:text-white"
          aria-label={collapsed ? "Expand plan" : "Collapse plan"}
        >
          <ChevronDown className={`h-4 w-4 transition-transform ${collapsed ? "-rotate-90" : "rotate-0"}`} />
        </button>
      </div>

      {!collapsed && (
        <div className="max-h-[28vh] overflow-y-auto p-4">
          {!hasSteps ? (
            <p className="text-sm text-white/55">Pipeline steps will appear when the run starts.</p>
          ) : (
            <ol className="space-y-2">
              {steps.map((step, index) => {
                const label = STATUS_LABEL[step.status] ?? step.status;
                const last = index === steps.length - 1;
                return (
                  <li key={step.id} className="relative rounded-md border border-white/10 bg-black/25 p-3">
                    {!last && <span className="absolute left-[19px] top-8 h-5 w-px bg-white/15" aria-hidden />}

                    <div className="flex items-start gap-3">
                      <span className="mt-0.5">{statusIcon(step.status)}</span>
                      <div className="min-w-0 flex-1">
                        <p className="truncate text-sm font-medium text-white/92">{step.title}</p>
                        {!readOnly && step.description && (
                          <p className="mt-1 text-xs text-white/50">{step.description}</p>
                        )}
                      </div>
                      <span className={`shrink-0 rounded-full border px-2 py-0.5 text-[11px] ${statusBadgeClass(step.status)}`}>
                        {label}
                      </span>
                    </div>
                  </li>
                );
              })}
            </ol>
          )}
        </div>
      )}
    </div>
  );
}
