"use client";

import { memo, useEffect, useState } from "react";
import { bundledLanguages, codeToHtml, isSpecialLang, type BundledLanguage, type SpecialLanguage } from "shiki";
import { Clipboard } from "lucide-react";
import { createScopedLogger } from "@/lib/logger";
import { cn } from "@/lib/utils";
import styles from "./CodeBlock.module.css";

const logger = createScopedLogger("CodeBlock");

interface CodeBlockProps {
  className?: string;
  code: string;
  language?: BundledLanguage | SpecialLanguage;
  theme?: "light-plus" | "dark-plus";
  disableCopy?: boolean;
}

export const CodeBlock = memo(function CodeBlock({
  className,
  code,
  language = "plaintext",
  theme = "dark-plus",
  disableCopy = false,
}: CodeBlockProps) {
  const [html, setHtml] = useState<string>();
  const [copied, setCopied] = useState(false);

  const copyToClipboard = () => {
    if (copied) {
      return;
    }

    navigator.clipboard.writeText(code);
    setCopied(true);

    setTimeout(() => {
      setCopied(false);
    }, 2000);
  };

  useEffect(() => {
    let isActive = true;

    if (language && !isSpecialLang(language) && !(language in bundledLanguages)) {
      logger.warn(`Unsupported language '${language}'`);
    }

    const processCode = async () => {
      const rendered = await codeToHtml(code, { lang: language, theme });

      if (isActive) {
        setHtml(rendered);
      }
    };

    void processCode();

    return () => {
      isActive = false;
    };
  }, [code, language, theme]);

  return (
    <div className={cn("group relative text-left", className)}>
      <div
        className={cn(
          styles.copyButtonContainer,
          "absolute top-[10px] right-[10px] z-10 flex items-center justify-center rounded-md bg-black/70 text-lg opacity-0 transition-opacity group-hover:opacity-100",
          copied && "rounded-l-none opacity-100",
        )}
      >
        {!disableCopy && (
          <button
            className={cn(
              "flex items-center justify-center bg-transparent p-[6px] text-white/70 hover:text-white before:rounded-l-md before:border-r before:border-white/10 before:bg-black/70 before:text-white/80",
              copied ? "before:opacity-100" : "before:opacity-0",
            )}
            title="Copy Code"
            type="button"
            onClick={copyToClipboard}
          >
            <Clipboard className="h-4 w-4" />
          </button>
        )}
      </div>
      <div dangerouslySetInnerHTML={{ __html: html ?? "" }} />
    </div>
  );
});
