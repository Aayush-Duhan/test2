"use client";

import * as React from "react";
import { FileTree, type FlatFileEntry } from "@/components/ui/file-tree";

/**
 * Demo page showcasing the FileTree component with static sample data.
 */

const sampleEntries: FlatFileEntry[] = [
  { path: "node_modules/zag-js/index.js", size: 1200 },
  { path: "node_modules/pandacss/index.js", size: 3400 },
  { path: "node_modules/@types/react/index.d.ts", size: 52000 },
  { path: "node_modules/@types/react-dom/index.d.ts", size: 18700 },
  { path: "src/app.tsx", size: 890 },
  { path: "src/index.ts", size: 120 },
  { path: "src/lib/utils.ts", size: 450 },
  { path: "src/components/header.tsx", size: 2100 },
  { path: "src/components/footer.tsx", size: 1500 },
  { path: "panda.config.ts", size: 340 },
  { path: "package.json", size: 1100 },
  { path: "renovate.json", size: 250 },
  { path: "README.md", size: 4200 },
  { path: "tsconfig.json", size: 680 },
  { path: ".gitignore", size: 120 },
];

export default function FileTreeDemo() {
  const [selected, setSelected] = React.useState<Set<string>>(new Set());
  const [search, setSearch] = React.useState("");

  const handleToggle = (path: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  };

  const filtered = React.useMemo(() => {
    if (!search.trim()) return sampleEntries;
    const q = search.toLowerCase();
    return sampleEntries.filter((e) => e.path.toLowerCase().includes(q));
  }, [search]);

  return (
    <div className="mx-auto max-w-lg p-8">
      <h2 className="mb-4 text-lg font-semibold text-white">
        File Tree Demo
      </h2>

      <input
        type="text"
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        placeholder="Filter files…"
        className="mb-3 w-full rounded-lg border border-[#333] bg-[#1a1a1a] px-3 py-2 text-sm text-white placeholder-[#555] outline-none focus:border-[#4da5fc]"
      />

      <FileTree
        entries={filtered}
        selectedPaths={selected}
        onToggle={handleToggle}
        searchQuery={search}
      />

      <p className="mt-3 text-xs text-[#666]">
        Selected: {selected.size} file{selected.size !== 1 ? "s" : ""}
        {selected.size > 0 && (
          <span className="ml-2 text-[#4da5fc]">
            ({Array.from(selected).join(", ")})
          </span>
        )}
      </p>
    </div>
  );
}
