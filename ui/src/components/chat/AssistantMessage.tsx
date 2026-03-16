"use client";

import { memo } from "react";
import { Markdown } from "./Markdown";

interface AssistantMessageProps {
  content: string;
}

export const AssistantMessage = memo(function AssistantMessage({ content }: AssistantMessageProps) {
  return (
    <div className="w-full overflow-hidden">
      <Markdown html>{content}</Markdown>
    </div>
  );
});
