"use client";

import { memo, useMemo } from "react";
import ReactMarkdown, { type Components } from "react-markdown";
import type { BundledLanguage } from "shiki";
import { createScopedLogger } from "@/lib/logger";
import { allowedHTMLElements, rehypePlugins, remarkPlugins } from "@/lib/markdown";
import { Artifact } from "./Artifact";
import { CodeBlock } from "./CodeBlock";
import styles from "./Markdown.module.css";

const logger = createScopedLogger("MarkdownComponent");

interface MarkdownProps {
  children: string;
  html?: boolean;
  limitedMarkdown?: boolean;
}

export const Markdown = memo(function Markdown({
  children,
  html = false,
  limitedMarkdown = false,
}: MarkdownProps) {
  logger.trace("Render");

  const components = useMemo(() => {
    return {
      div: ({ className, children, node, ...props }) => {
        if (className?.includes("__boltArtifact__")) {
          const messageId =
            typeof node?.properties?.dataMessageId === "string"
              ? node.properties.dataMessageId
              : typeof node?.properties?.["data-message-id"] === "string"
                ? node.properties["data-message-id"]
                : "";

          if (!messageId) {
            logger.error("Invalid message id for artifact placeholder");
          }

          return <Artifact messageId={messageId} />;
        }

        return (
          <div className={className} {...props}>
            {children}
          </div>
        );
      },
      pre: (props) => {
        const { children, node, ...rest } = props;
        const [firstChild] = node?.children ?? [];

        if (
          firstChild &&
          firstChild.type === "element" &&
          firstChild.tagName === "code" &&
          firstChild.children[0]?.type === "text"
        ) {
          const { className, ...codeProps } = firstChild.properties;
          const [, language = "plaintext"] = /language-(\w+)/.exec(String(className) || "") ?? [];

          return (
            <CodeBlock
              code={String(firstChild.children[0].value)}
              language={language as BundledLanguage}
              {...codeProps}
            />
          );
        }

        return <pre {...rest}>{children}</pre>;
      },
    } satisfies Components;
  }, []);

  return (
    <ReactMarkdown
      allowedElements={[...allowedHTMLElements]}
      className={styles.markdownContent}
      components={components}
      remarkPlugins={remarkPlugins(limitedMarkdown)}
      rehypePlugins={rehypePlugins(html)}
    >
      {children}
    </ReactMarkdown>
  );
});
