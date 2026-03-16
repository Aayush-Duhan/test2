"use client";

import { useStore } from "@nanostores/react";
import { memo, useMemo, useState } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import { workbenchStore } from "@/lib/workbench-store";
import { CodeBlock } from "./CodeBlock";

interface ArtifactProps {
  messageId: string;
}

export const Artifact = memo(function Artifact({ messageId }: ArtifactProps) {
  const [showActions, setShowActions] = useState(true);
  const artifacts = useStore(workbenchStore.artifacts);
  const artifact = artifacts[messageId];
  const actionsStore = artifact?.runner.actions;
  const actions = useStore(actionsStore ?? workbenchStore.artifacts);

  const actionList = useMemo(() => {
    if (!artifact || !actionsStore) {
      return [];
    }

    return Object.values(actions as Record<string, { type?: string; content?: string; filePath?: string; status?: string }>);
  }, [actions, actionsStore, artifact]);

  if (!artifact) {
    return null;
  }

  return (
    <div className="artifact w-full overflow-hidden rounded-lg border border-white/10 bg-white/[0.03]">
      <div className="flex items-stretch">
        <button
          className="w-full px-5 py-3.5 text-left transition-colors hover:bg-white/[0.04]"
          type="button"
          onClick={() => workbenchStore.setShowWorkbench(!workbenchStore.showWorkbench.get())}
        >
          <div className="text-sm font-medium text-white/90">{artifact.title}</div>
          <div className="mt-0.5 text-xs text-white/55">Click to open Workbench</div>
        </button>
        {actionList.length > 0 && (
          <button
            className="border-l border-white/10 px-4 text-white/60 transition-colors hover:bg-white/[0.04] hover:text-white"
            type="button"
            onClick={() => setShowActions((current) => !current)}
          >
            {showActions ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </button>
        )}
      </div>

      {showActions && actionList.length > 0 && (
        <div className="border-t border-white/10 bg-black/20 px-5 py-4">
          <ul className="space-y-3">
            {actionList.map((action, index) => (
              <li key={`${messageId}-${index}`} className="text-sm text-white/80">
                {action.type === "file" ? (
                  <div>
                    Create <code className="rounded bg-white/10 px-1.5 py-1 text-xs text-amber-100">{action.filePath}</code>
                  </div>
                ) : (
                  <div>
                    <div className="mb-2">Run command</div>
                    <CodeBlock code={action.content ?? ""} disableCopy language="shell" />
                  </div>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
});
