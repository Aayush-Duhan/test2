"use client";

export function Header() {
  return (
    <header className="flex h-[var(--header-h)] w-full shrink-0 items-center border-b border-white/10 bg-[#141414] px-4">
      <img src="/EY.svg" alt="Ethan logo" className="h-7 w-7" />
      <span className="ml-3 text-lg font-bold text-white">
        ETHAN
      </span>
    </header>
  );
}
