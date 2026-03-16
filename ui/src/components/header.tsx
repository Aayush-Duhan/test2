"use client";

import Image from "next/image";
import { useStore } from "@nanostores/react";
import { Code2 } from "lucide-react";
import { workbenchStore } from "@/lib/workbench-store";
import { cn } from "@/lib/utils";

interface HeaderProps {
  showWorkbenchToggle?: boolean;
}

export function Header({ showWorkbenchToggle = false }: HeaderProps) {
  const showWorkbench = useStore(workbenchStore.showWorkbench);

  return (
    <header className="flex h-[var(--header-h)] w-full shrink-0 items-center border-b border-white/10 bg-[#141414] px-4">
      <div className="flex items-center">
        <Image src="/EY.svg" alt="Ethan logo" width={28} height={28} className="h-7 w-7" priority />
        <span className="ml-3 text-lg font-bold text-white">
          ETHAN
        </span>
      </div>

      <div className="ml-auto">
        {showWorkbenchToggle && (
          <div className="overflow-hidden rounded-md border border-white/15">
            <button
              type="button"
              title={showWorkbench ? "Hide Workbench" : "Show Workbench"}
              aria-pressed={showWorkbench}
              onClick={() => workbenchStore.setShowWorkbench(!showWorkbench)}
              className={cn(
                "flex items-center p-1.5 transition-colors",
                showWorkbench
                  ? "bg-[#1f3651] text-[#9fceff]"
                  : "bg-transparent text-white/60 hover:bg-white/6 hover:text-white",
              )}
            >
              <Code2 className="h-4 w-4" />
            </button>
          </div>
        )}
      </div>
    </header>
  );
}
