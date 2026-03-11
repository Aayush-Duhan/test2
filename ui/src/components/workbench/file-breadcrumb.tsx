"use client";

/**
 * FileBreadcrumb — interactive file path breadcrumb with dropdown navigation.
 *
 * Adapted from bolt.new's FileBreadcrumb.tsx for the dark theme.
 * Uses @radix-ui/react-dropdown-menu for the dropdown,
 * and framer-motion for animations.
 */

import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import { AnimatePresence, motion, type Variants } from 'framer-motion';
import { memo, useEffect, useRef, useState } from 'react';
import { FileCode, ChevronRight } from 'lucide-react';
import type { FileMap } from '@/lib/stores/files-store';
import { cubicEasingFn } from '@/lib/utils/easings';
import { cn } from '@/lib/utils';
import { FileTree } from './file-tree';

const WORK_DIR = '/project';
const WORK_DIR_REGEX = new RegExp(
  `^${WORK_DIR.split('/').slice(0, -1).join('/').replaceAll('/', '\\/')}/`,
);

const contextMenuVariants = {
  open: {
    y: 0,
    opacity: 1,
    transition: {
      duration: 0.15,
      ease: cubicEasingFn,
    },
  },
  close: {
    y: 6,
    opacity: 0,
    transition: {
      duration: 0.15,
      ease: cubicEasingFn,
    },
  },
} satisfies Variants;

interface FileBreadcrumbProps {
  files?: FileMap;
  pathSegments?: string[];
  onFileSelect?: (filePath: string) => void;
}

export const FileBreadcrumb = memo<FileBreadcrumbProps>(
  ({ files, pathSegments = [], onFileSelect }) => {
    const [activeIndex, setActiveIndex] = useState<number | null>(null);

    const contextMenuRef = useRef<HTMLDivElement | null>(null);
    const segmentRefs = useRef<(HTMLSpanElement | null)[]>([]);

    const handleSegmentClick = (index: number) => {
      setActiveIndex((prevIndex) => (prevIndex === index ? null : index));
    };

    useEffect(() => {
      const handleOutsideClick = (event: MouseEvent) => {
        if (
          activeIndex !== null &&
          !contextMenuRef.current?.contains(event.target as Node) &&
          !segmentRefs.current.some((ref) => ref?.contains(event.target as Node))
        ) {
          setActiveIndex(null);
        }
      };

      document.addEventListener('mousedown', handleOutsideClick);

      return () => {
        document.removeEventListener('mousedown', handleOutsideClick);
      };
    }, [activeIndex]);

    if (files === undefined || pathSegments.length === 0) {
      return null;
    }

    return (
      <div className="flex">
        {pathSegments.map((segment, index) => {
          const isLast = index === pathSegments.length - 1;

          const path = pathSegments.slice(0, index).join('/');

          if (!WORK_DIR_REGEX.test(path)) {
            return null;
          }

          const isActive = activeIndex === index;

          return (
            <div key={index} className="relative flex items-center">
              <DropdownMenu.Root open={isActive} modal={false}>
                <DropdownMenu.Trigger asChild>
                  <span
                    ref={(ref) => {
                      segmentRefs.current[index] = ref;
                    }}
                    className={cn(
                      'flex items-center gap-1.5 cursor-pointer shrink-0',
                      {
                        'text-white/50 hover:text-white': !isActive,
                        'text-white underline': isActive,
                        'pr-4': isLast,
                      },
                    )}
                    onClick={() => handleSegmentClick(index)}
                  >
                    {isLast && <FileCode className="h-3.5 w-3.5" />}
                    {segment}
                  </span>
                </DropdownMenu.Trigger>
                {index > 0 && !isLast && (
                  <ChevronRight className="inline-block mx-1 h-3 w-3 text-white/30" />
                )}
                <AnimatePresence>
                  {isActive && (
                    <DropdownMenu.Portal>
                      <DropdownMenu.Content
                        className="z-file-tree-breadcrumb"
                        asChild
                        align="start"
                        side="bottom"
                        avoidCollisions={false}
                      >
                        <motion.div
                          ref={contextMenuRef}
                          initial="close"
                          animate="open"
                          exit="close"
                          variants={contextMenuVariants}
                        >
                          <div className="rounded-lg overflow-hidden">
                            <div className="max-h-[50vh] min-w-[300px] overflow-scroll bg-[#0d0d0d] border border-white/10 shadow-lg rounded-lg">
                              <FileTree
                                files={files}
                                hideRoot
                                rootFolder={path}
                                collapsed
                                allowFolderSelection
                                selectedFile={`${path}/${segment}`}
                                onFileSelect={(filePath) => {
                                  setActiveIndex(null);
                                  onFileSelect?.(filePath);
                                }}
                              />
                            </div>
                          </div>
                        </motion.div>
                      </DropdownMenu.Content>
                    </DropdownMenu.Portal>
                  )}
                </AnimatePresence>
              </DropdownMenu.Root>
            </div>
          );
        })}
      </div>
    );
  },
);

FileBreadcrumb.displayName = 'FileBreadcrumb';
