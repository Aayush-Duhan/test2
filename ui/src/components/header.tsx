"use client";

import Image from "next/image";

export function Header() {
  return (
    <header className="flex h-[var(--header-h)] w-full shrink-0 items-center border-b border-white/10 bg-[#141414] px-4">
      <Image src="/EY.svg" alt="Ethan logo" width={28} height={28} className="h-7 w-7" priority />
      <span className="ml-3 text-lg font-bold text-white">
        ETHAN
      </span>
    </header>
  );
}
