"use client";

import { memo } from "react";
import { Markdown } from "./Markdown";

interface UserMessageProps {
  content: string;
}

export const UserMessage = memo(function UserMessage({ content }: UserMessageProps) {
  return (
    <div className="overflow-hidden text-right">
      <Markdown limitedMarkdown>{content}</Markdown>
    </div>
  );
});
