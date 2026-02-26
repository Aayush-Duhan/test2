"use client";

import * as React from "react";
import { ChevronLeft, ChevronRight, Check, Database, FileText, GitBranch, CheckCircle2 } from "lucide-react";
import {
  useWizardState,
  WIZARD_STEPS,
  SOURCE_LANGUAGES,
  type WizardStepId,
  type WizardFile,
  goToNextStep,
  goToPreviousStep,
  setSourceLanguage,
  addSourceFiles,
  removeSourceFile,
  addMappingFiles,
  removeMappingFile,
  setStarting,
  canProceedToNext,
  isFirstStep,
  isLastStep,
  resetWizard,
} from "@/lib/wizard-store";

// Utility for class names
function cn(...inputs: (string | boolean | undefined | null)[]): string {
  return inputs.filter(Boolean).join(" ");
}

// Step icons
const STEP_ICONS: Record<WizardStepId, React.ElementType> = {
  language: Database,
  files: FileText,
  mapping: GitBranch,
  summary: CheckCircle2,
};

// Step 1: Language Selection
const LanguageStep = React.memo(function LanguageStep() {
  const { sourceLanguage } = useWizardState();

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold text-white mb-2">Select Source Database</h3>
        <p className="text-sm text-[#8a8a8f] mb-4">
          Choose the database platform you're migrating from.
        </p>
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
        {SOURCE_LANGUAGES.map((lang) => (
          <button
            key={lang.id}
            onClick={() => setSourceLanguage(lang.id)}
            className={cn(
              "p-4 rounded-lg border text-left transition-all duration-200",
              sourceLanguage === lang.id
                ? "border-[#4da5fc] bg-[#4da5fc]/10 text-white"
                : "border-[#333] bg-[#1a1a1a] text-[#8a8a8f] hover:border-[#444] hover:text-white"
            )}
          >
            <span className="text-sm font-medium">{lang.label}</span>
          </button>
        ))}
      </div>
    </div>
  );
});

// Step 2: File Selection
const FilesStep = React.memo(function FilesStep() {
  const { sourceFiles } = useWizardState();
  const fileInputRef = React.useRef<HTMLInputElement>(null);

  const handleFileUpload = React.useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files) return;

    const uploadedFiles: WizardFile[] = Array.from(files).map((file) => ({
      name: file.name,
      path: file.webkitRelativePath || file.name,
      relativePath: file.webkitRelativePath || file.name,
      file: file,
    }));

    addSourceFiles(uploadedFiles);
    event.target.value = "";
  }, []);

  const handleDrop = React.useCallback((event: React.DragEvent) => {
    event.preventDefault();
    const items = event.dataTransfer.items;
    const uploadedFiles: WizardFile[] = [];

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      if (item.kind === "file") {
        const file = item.getAsFile();
        if (file) {
          uploadedFiles.push({
            name: file.name,
            path: file.name,
            relativePath: file.name,
            file: file,
          });
        }
      }
    }

    addSourceFiles(uploadedFiles);
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold text-white mb-2">Upload Source Files</h3>
        <p className="text-sm text-[#8a8a8f] mb-4">
          Upload your SQL scripts, DDL files, or stored procedures to migrate.
        </p>
      </div>

      {/* Drop zone */}
      <div
        onDrop={handleDrop}
        onDragOver={(e) => e.preventDefault()}
        onClick={() => fileInputRef.current?.click()}
        className="border-2 border-dashed border-[#333] rounded-lg p-8 text-center hover:border-[#4da5fc] transition-colors cursor-pointer"
      >
        <input
          ref={fileInputRef}
          type="file"
          multiple
          accept=".sql,.ddl,.btq,.txt"
          onChange={handleFileUpload}
          className="hidden"
        />
        <FileText className="w-12 h-12 mx-auto mb-4 text-[#4da5fc]" />
        <p className="text-white font-medium mb-1">Drop files here or click to browse</p>
        <p className="text-sm text-[#8a8a8f]">.sql, .ddl, .btq, .txt files supported</p>
      </div>

      {/* File list */}
      {sourceFiles.length > 0 && (
        <div className="space-y-2">
          <p className="text-sm font-medium text-white">{sourceFiles.length} file(s) selected</p>
          <div className="max-h-48 overflow-y-auto space-y-1 scrollbar-dark">
            {sourceFiles.map((file) => (
              <div
                key={file.relativePath ?? file.name}
                className="flex items-center justify-between p-2 bg-[#1a1a1a] rounded border border-[#333]"
              >
                <span className="text-sm text-[#ccc] truncate flex-1">
                  {file.relativePath ?? file.name}
                </span>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    removeSourceFile(file.relativePath ?? file.name);
                  }}
                  className="ml-2 text-[#8a8a8f] hover:text-red-400 transition-colors"
                >
                  ×
                </button>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
});

// Step 3: Schema Mapping
const MappingStep = React.memo(function MappingStep() {
  const { mappingFiles } = useWizardState();
  const fileInputRef = React.useRef<HTMLInputElement>(null);

  const handleFileUpload = React.useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files) return;

    const uploadedFiles: WizardFile[] = Array.from(files).map((file) => ({
      name: file.name,
      path: file.name,
      relativePath: file.name,
      file: file,
    }));

    addMappingFiles(uploadedFiles);
    event.target.value = "";
  }, []);

  const handleDrop = React.useCallback((event: React.DragEvent) => {
    event.preventDefault();
    const items = event.dataTransfer.items;
    const uploadedFiles: WizardFile[] = [];

    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      if (item.kind === "file") {
        const file = item.getAsFile();
        if (file) {
          uploadedFiles.push({
            name: file.name,
            path: file.name,
            relativePath: file.name,
            file: file,
          });
        }
      }
    }

    addMappingFiles(uploadedFiles);
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold text-white mb-2">Schema Mapping (Optional)</h3>
        <p className="text-sm text-[#8a8a8f] mb-4">
          Upload CSV files containing table and column name mappings for your migration.
        </p>
      </div>

      {/* Drop zone */}
      <div
        onDrop={handleDrop}
        onDragOver={(e) => e.preventDefault()}
        onClick={() => fileInputRef.current?.click()}
        className="border-2 border-dashed border-[#333] rounded-lg p-8 text-center hover:border-[#4da5fc] transition-colors cursor-pointer"
      >
        <input
          ref={fileInputRef}
          type="file"
          multiple
          accept=".csv,.json"
          onChange={handleFileUpload}
          className="hidden"
        />
        <GitBranch className="w-12 h-12 mx-auto mb-4 text-[#4da5fc]" />
        <p className="text-white font-medium mb-1">Drop CSV/JSON files here or click to browse</p>
        <p className="text-sm text-[#8a8a8f]">.csv, .json mapping files supported</p>
      </div>

      {/* File list */}
      {mappingFiles.length > 0 && (
        <div className="space-y-2">
          <p className="text-sm font-medium text-white">{mappingFiles.length} mapping file(s) selected</p>
          <div className="max-h-48 overflow-y-auto space-y-1 scrollbar-dark">
            {mappingFiles.map((file) => (
              <div
                key={file.relativePath ?? file.name}
                className="flex items-center justify-between p-2 bg-[#1a1a1a] rounded border border-[#333]"
              >
                <span className="text-sm text-[#ccc] truncate flex-1">
                  {file.relativePath ?? file.name}
                </span>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    removeMappingFile(file.relativePath ?? file.name);
                  }}
                  className="ml-2 text-[#8a8a8f] hover:text-red-400 transition-colors"
                >
                  ×
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      <p className="text-xs text-[#666]">
        Skip this step if you don't have schema mapping files. You can provide mappings later.
      </p>
    </div>
  );
});

// Step 4: Summary
const SummaryStep = React.memo(function SummaryStep() {
  const { sourceLanguage, sourceFiles, mappingFiles, isStarting, startError } = useWizardState();

  const language = SOURCE_LANGUAGES.find((l) => l.id === sourceLanguage);

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-semibold text-white mb-2">Review & Start Migration</h3>
        <p className="text-sm text-[#8a8a8f] mb-4">
          Review your configuration before starting the migration.
        </p>
      </div>

      <div className="space-y-4">
        {/* Language */}
        <div className="p-4 bg-[#1a1a1a] rounded-lg border border-[#333]">
          <div className="flex items-center gap-3">
            <Database className="w-5 h-5 text-[#4da5fc]" />
            <div>
              <p className="text-xs text-[#8a8a8f]">Source Database</p>
              <p className="text-sm font-medium text-white">{language?.label}</p>
            </div>
          </div>
        </div>

        {/* Files */}
        <div className="p-4 bg-[#1a1a1a] rounded-lg border border-[#333]">
          <div className="flex items-center gap-3 mb-2">
            <FileText className="w-5 h-5 text-[#4da5fc]" />
            <div>
              <p className="text-xs text-[#8a8a8f]">Source Files</p>
              <p className="text-sm font-medium text-white">{sourceFiles.length} file(s)</p>
            </div>
          </div>
          {sourceFiles.length > 0 && (
            <div className="ml-8 text-xs text-[#666] max-h-24 overflow-y-auto scrollbar-dark">
              {sourceFiles.slice(0, 5).map((f) => (
                <p key={f.relativePath} className="truncate">
                  {f.relativePath ?? f.name}
                </p>
              ))}
              {sourceFiles.length > 5 && (
                <p className="text-[#8a8a8f]">...and {sourceFiles.length - 5} more</p>
              )}
            </div>
          )}
        </div>

        {/* Mapping */}
        <div className="p-4 bg-[#1a1a1a] rounded-lg border border-[#333]">
          <div className="flex items-center gap-3">
            <GitBranch className="w-5 h-5 text-[#4da5fc]" />
            <div>
              <p className="text-xs text-[#8a8a8f]">Schema Mappings</p>
              <p className="text-sm font-medium text-white">
                {mappingFiles.length > 0 ? `${mappingFiles.length} mapping file(s)` : "None (optional)"}
              </p>
            </div>
          </div>
          {mappingFiles.length > 0 && (
            <div className="ml-8 mt-2 text-xs text-[#666] max-h-24 overflow-y-auto scrollbar-dark">
              {mappingFiles.map((f) => (
                <p key={f.relativePath} className="truncate">
                  {f.relativePath ?? f.name}
                </p>
              ))}
            </div>
          )}
        </div>
      </div>

      {startError && (
        <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg">
          <p className="text-sm text-red-400">{startError}</p>
        </div>
      )}

      {isStarting && (
        <div className="flex items-center justify-center gap-2 p-4">
          <div className="w-4 h-4 border-2 border-[#4da5fc] border-t-transparent rounded-full animate-spin" />
          <p className="text-sm text-[#8a8a8f]">Starting migration...</p>
        </div>
      )}
    </div>
  );
});

// Step content renderer
const StepContent = React.memo(function StepContent({ step }: { step: WizardStepId }) {
  switch (step) {
    case "language":
      return <LanguageStep />;
    case "files":
      return <FilesStep />;
    case "mapping":
      return <MappingStep />;
    case "summary":
      return <SummaryStep />;
    default:
      return null;
  }
});

// Props for the main wizard component
interface SetupWizardProps {
  onStartMigration: () => void | Promise<void>;
  isBusy?: boolean;
}

// Main Wizard Component
export const SetupWizard = React.memo(function SetupWizard({ onStartMigration, isBusy = false }: SetupWizardProps) {
  const { currentStep, completedSteps, sourceFiles, isStarting } = useWizardState();
  const canProceed = canProceedToNext();
  const first = isFirstStep();
  const last = isLastStep();

  const handleNext = async () => {
    if (last) {
      setStarting(true);
      try {
        await onStartMigration();
        // Reset starting state after successful migration start
        setStarting(false);
      } catch (error) {
        setStarting(false, error instanceof Error ? error.message : "Failed to start migration");
      }
    } else {
      goToNextStep();
    }
  };

  const handleBack = () => {
    if (!first) {
      goToPreviousStep();
    }
  };

  return (
    <div className="w-full max-w-2xl mx-auto">
      {/* Step indicator */}
      <div className="mb-8">
        <div className="grid grid-cols-4 items-start">
          {WIZARD_STEPS.map((step, index) => {
            const Icon = STEP_ICONS[step.id];
            const isActive = step.id === currentStep;
            const isCompleted = completedSteps.includes(step.id);

            return (
              <div key={step.id} className="relative flex flex-col items-center px-2">
                {index < WIZARD_STEPS.length - 1 && (
                  <div
                    className={cn(
                      "absolute top-5 left-1/2 ml-6 h-0.5 w-[calc(100%-3rem)]",
                      completedSteps.includes(step.id) ? "bg-green-500/30" : "bg-[#333]"
                    )}
                  />
                )}

                <div
                  className={cn(
                    "relative z-10 w-10 h-10 rounded-full flex items-center justify-center transition-all",
                    isActive
                      ? "bg-[#4da5fc] text-white"
                      : isCompleted
                      ? "bg-green-500/20 text-green-400 border border-green-500/30"
                      : "bg-[#1a1a1a] text-[#666] border border-[#333]"
                  )}
                >
                  {isCompleted ? (
                    <Check className="w-5 h-5" />
                  ) : (
                    <Icon className="w-5 h-5" />
                  )}
                </div>

                <p
                  className={cn(
                    "mt-2 min-h-8 w-full text-center text-xs font-medium hidden sm:block",
                    isActive ? "text-white" : "text-[#666]"
                  )}
                >
                  {step.label}
                </p>
              </div>
            );
          })}
        </div>
      </div>

      {/* Step content */}
      <div className="bg-[#0d0d0d] rounded-xl border border-[#333] p-6 mb-6">
        <StepContent step={currentStep} />
      </div>

      {/* Navigation */}
      <div className="flex items-center justify-between">
        <button
          onClick={handleBack}
          disabled={first}
          className={cn(
            "flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-all",
            first
              ? "text-[#333] cursor-not-allowed"
              : "text-[#8a8a8f] hover:text-white hover:bg-[#1a1a1a]"
          )}
        >
          <ChevronLeft className="w-4 h-4" />
          Back
        </button>

        <button
          onClick={handleNext}
          disabled={!canProceed || isBusy || isStarting}
          className={cn(
            "flex items-center gap-2 px-6 py-2 rounded-lg font-medium transition-all",
            canProceed && !isBusy && !isStarting
              ? "bg-[#4da5fc] text-white hover:bg-[#3d8fd6]"
              : "bg-[#333] text-[#666] cursor-not-allowed"
          )}
        >
          {last ? "Start Migration" : "Next"}
          {!last && <ChevronRight className="w-4 h-4" />}
        </button>
      </div>
    </div>
  );
});

// Export reset function for external use
export { resetWizard };
