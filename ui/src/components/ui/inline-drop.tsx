'use client'

// Full credits to : https://github.com/haaarshsingh
// https://ui.harshsingh.xyz/inline-dropdown

import { cn } from '@/lib/utils'


type InlineDropdownProps = {
  value?: string
  onSelect?: (value: string) => void
}

export const InlineDropdownS = ({ value, onSelect }: InlineDropdownProps) => {

  const script = [
    "DDL",
    "DML",
    "DCL",
    "TCL"
  ]

  return (
    <div className="flex w-60 flex-col items-start gap-y-1 rounded-lg border border-neutral-300 bg-neutral-200/10 p-1 dark:border-neutral-700 dark:bg-neutral-800">
      <div className="scrollbar-dark max-h-52 w-full overflow-y-auto pr-1">
        {script.map((script) => (
          <button
            key={script}
            onClick={() => onSelect?.(script)}
            className={cn(
              "flex w-full items-center justify-between rounded px-2 py-1.5 text-xs text-neutral-100 hover:bg-neutral-950/10 active:bg-neutral-50/15 dark:hover:bg-neutral-50/10",
              value === script && "bg-neutral-950/10 dark:bg-neutral-50/10"
            )}
          >
            {script}
          </button>
        ))}
      </div>
    </div>
  )
}
