import { useRef, useCallback, useState } from 'react';

export function useSnapScroll() {
  const autoScrollRef = useRef(true);
  const scrollNodeRef = useRef<HTMLDivElement | null>(null);
  const onScrollRef = useRef<(() => void) | null>(null);
  const observerRef = useRef<ResizeObserver | null>(null);
  const [isAtBottom, setIsAtBottom] = useState(true);

  const messageRef = useCallback((node: HTMLDivElement | null) => {
    if (node) {
      const observer = new ResizeObserver(() => {
        if (autoScrollRef.current && scrollNodeRef.current) {
          const { scrollHeight, clientHeight } = scrollNodeRef.current;
          const scrollTarget = scrollHeight - clientHeight;

          scrollNodeRef.current.scrollTo({
            top: scrollTarget,
            behavior: 'smooth',
          });
        }
      });

      observer.observe(node);
      observerRef.current = observer;
    } else {
      observerRef.current?.disconnect();
      observerRef.current = null;
    }
  }, []);

  const scrollRef = useCallback((node: HTMLDivElement | null) => {
    if (node) {
      onScrollRef.current = () => {
        const { scrollTop, scrollHeight, clientHeight } = node;
        const scrollTarget = scrollHeight - clientHeight;

        const atBottom = Math.abs(scrollTop - scrollTarget) <= 10;
        autoScrollRef.current = atBottom;
        setIsAtBottom(atBottom);
      };

      node.addEventListener('scroll', onScrollRef.current);

      scrollNodeRef.current = node;
    } else {
      if (onScrollRef.current) {
        scrollNodeRef.current?.removeEventListener('scroll', onScrollRef.current);
      }

      scrollNodeRef.current = null;
      onScrollRef.current = null;
    }
  }, []);

  const scrollToBottom = useCallback(() => {
    if (scrollNodeRef.current) {
      const { scrollHeight, clientHeight } = scrollNodeRef.current;
      scrollNodeRef.current.scrollTo({
        top: scrollHeight - clientHeight,
        behavior: 'smooth',
      });
      autoScrollRef.current = true;
      setIsAtBottom(true);
    }
  }, []);

  return [messageRef, scrollRef, isAtBottom, scrollToBottom] as const;
}
