"use client";

import * as React from "react";
import { SendHorizontal } from "lucide-react";

type ClassValue = string | number | boolean | null | undefined;
function cn(...inputs: ClassValue[]): string {
  return inputs.filter(Boolean).join(" ");
}

type PromptBoxProps = React.TextareaHTMLAttributes<HTMLTextAreaElement> & {
  onSend?: (value: string) => void;
  actionLabel?: string;
  isSending?: boolean;
};

export const PromptBox = React.forwardRef<HTMLTextAreaElement, PromptBoxProps>(
  ({ className, onSend, actionLabel, isSending = false, ...props }, ref) => {
    const internalTextareaRef = React.useRef<HTMLTextAreaElement>(null);
    const [value, setValue] = React.useState("");

    React.useImperativeHandle(ref, () => internalTextareaRef.current!, []);

    React.useLayoutEffect(() => {
      const textarea = internalTextareaRef.current;
      if (!textarea) return;
      textarea.style.height = "auto";
      textarea.style.height = `${Math.min(textarea.scrollHeight, 200)}px`;
    }, [value]);

    const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
      setValue(e.target.value);
      if (props.onChange) props.onChange(e);
    };

    const hasValue = value.trim().length > 0;
    const disabled = props.disabled || isSending || !hasValue || !onSend;

    const send = () => {
      if (disabled) return;
      onSend?.(value.trim());
      setValue("");
    };

    const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        send();
      }
      props.onKeyDown?.(e);
    };

    return (
      <div className={cn("flex flex-col rounded-[20px] border border-white/10 bg-[#141414] p-3", className)}>
        <textarea
          ref={internalTextareaRef}
          rows={1}
          value={value}
          onChange={handleInputChange}
          onKeyDown={handleKeyDown}
          placeholder={props.placeholder ?? "Ask the agent to fix issues in converted code..."}
          className="custom-scrollbar min-h-12 w-full resize-none border-0 bg-transparent px-2 py-2 text-base text-white placeholder:text-white/45 focus-visible:outline-none"
          {...props}
        />

        <div className="mt-2 flex items-center justify-end">
          <button
            type="button"
            disabled={disabled}
            onClick={send}
            className="flex h-9 items-center gap-2 rounded-full bg-white px-4 text-sm font-medium text-black transition-colors hover:bg-white/90 disabled:cursor-not-allowed disabled:opacity-45"
          >
            <span>{actionLabel ?? "Send"}</span>
            <SendHorizontal className="h-4 w-4" />
          </button>
        </div>
      </div>
    );
  },
);
PromptBox.displayName = "PromptBox";
