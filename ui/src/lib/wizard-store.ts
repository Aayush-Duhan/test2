import React from 'react';

// Source languages supported by the migration tool
export const SOURCE_LANGUAGES = [
  { id: 'SqlServer', label: 'SQL Server' },
  { id: 'Redshift', label: 'Amazon Redshift' },
  { id: 'Oracle', label: 'Oracle' },
  { id: 'Teradata', label: 'Teradata' },
  { id: 'BigQuery', label: 'Google BigQuery' },
  { id: 'Databricks', label: 'Databricks' },
  { id: 'Greenplum', label: 'Greenplum' },
  { id: 'Sybase', label: 'SAP Sybase' },
  { id: 'Postgresql', label: 'PostgreSQL' },
  { id: 'Netezza', label: 'IBM Netezza' },
  { id: 'Spark', label: 'Apache Spark' },
  { id: 'Vertica', label: 'Vertica' },
  { id: 'Hive', label: 'Apache Hive' },
  { id: 'Db2', label: 'IBM Db2' },
] as const;

export type SourceLanguage = (typeof SOURCE_LANGUAGES)[number]['id'];

// Wizard steps - 4 steps as requested
export const WIZARD_STEPS = [
  { id: 'language', label: 'Source Language', description: 'Select your source database' },
  { id: 'files', label: 'Source Files', description: 'Upload your SQL files' },
  { id: 'mapping', label: 'Schema Mapping', description: 'Upload schema mapping files' },
  { id: 'summary', label: 'Summary', description: 'Review and start migration' },
] as const;

export type WizardStepId = (typeof WIZARD_STEPS)[number]['id'];

// Uploaded file type
export interface WizardFile {
  name: string;
  path: string;
  relativePath: string;
  file?: File; // Keep reference to actual File object for upload
}

// Wizard state
export interface WizardState {
  currentStep: WizardStepId;
  completedSteps: WizardStepId[];
  
  // Step 1: Language
  sourceLanguage: SourceLanguage;
  
  // Step 2: Files
  sourceFiles: WizardFile[];
  
  // Step 3: Schema Mapping
  mappingFiles: WizardFile[];
  
  // Step 4: Summary
  isStarting: boolean;
  startError?: string;
}

// Initial state
const initialState: WizardState = {
  currentStep: 'language',
  completedSteps: [],
  sourceLanguage: 'Teradata',
  sourceFiles: [],
  mappingFiles: [],
  isStarting: false,
};

// Create a simple store using React state pattern
let state = { ...initialState };
const listeners = new Set<() => void>();

export function getWizardState(): WizardState {
  return state;
}

export function subscribeToWizard(callback: () => void): () => void {
  listeners.add(callback);
  return () => listeners.delete(callback);
}

function notifyListeners() {
  listeners.forEach(callback => callback());
}

// Actions
export function setStep(step: WizardStepId) {
  state = { ...state, currentStep: step };
  notifyListeners();
}

export function setSourceLanguage(language: SourceLanguage) {
  state = { ...state, sourceLanguage: language };
  notifyListeners();
}

export function setSourceFiles(files: WizardFile[]) {
  state = { ...state, sourceFiles: files };
  notifyListeners();
}

export function addSourceFiles(files: WizardFile[]) {
  const current = state.sourceFiles;
  const existing = new Set(current.map(f => f.relativePath ?? f.name));
  const newFiles = files.filter(f => !existing.has(f.relativePath ?? f.name));
  state = { ...state, sourceFiles: [...current, ...newFiles] };
  notifyListeners();
}

export function removeSourceFile(fileKey: string) {
  const current = state.sourceFiles;
  state = { ...state, sourceFiles: current.filter(f => (f.relativePath ?? f.name) !== fileKey) };
  notifyListeners();
}

export function setMappingFiles(files: WizardFile[]) {
  state = { ...state, mappingFiles: files };
  notifyListeners();
}

export function addMappingFiles(files: WizardFile[]) {
  const current = state.mappingFiles;
  const existing = new Set(current.map(f => f.relativePath ?? f.name));
  const newFiles = files.filter(f => !existing.has(f.relativePath ?? f.name));
  state = { ...state, mappingFiles: [...current, ...newFiles] };
  notifyListeners();
}

export function removeMappingFile(fileKey: string) {
  const current = state.mappingFiles;
  state = { ...state, mappingFiles: current.filter(f => (f.relativePath ?? f.name) !== fileKey) };
  notifyListeners();
}

export function setStarting(isStarting: boolean, error?: string) {
  state = { ...state, isStarting, startError: error };
  notifyListeners();
}

export function markStepCompleted(step: WizardStepId) {
  const current = state.completedSteps;
  if (!current.includes(step)) {
    state = { ...state, completedSteps: [...current, step] };
    notifyListeners();
  }
}

export function goToNextStep() {
  const { currentStep, completedSteps } = state;
  const currentIndex = WIZARD_STEPS.findIndex(s => s.id === currentStep);
  
  // Mark current step as completed
  if (!completedSteps.includes(currentStep)) {
    state = { ...state, completedSteps: [...completedSteps, currentStep] };
  }
  
  // Go to next step
  if (currentIndex < WIZARD_STEPS.length - 1) {
    state = { ...state, currentStep: WIZARD_STEPS[currentIndex + 1].id };
  }
  notifyListeners();
}

export function goToPreviousStep() {
  const { currentStep } = state;
  const currentIndex = WIZARD_STEPS.findIndex(s => s.id === currentStep);
  
  if (currentIndex > 0) {
    state = { ...state, currentStep: WIZARD_STEPS[currentIndex - 1].id };
    notifyListeners();
  }
}

export function resetWizard() {
  state = { ...initialState };
  notifyListeners();
}

// Computed helpers
export function getCurrentStepIndex(): number {
  const { currentStep } = state;
  return WIZARD_STEPS.findIndex(s => s.id === currentStep);
}

export function isFirstStep(): boolean {
  return getCurrentStepIndex() === 0;
}

export function isLastStep(): boolean {
  return getCurrentStepIndex() === WIZARD_STEPS.length - 1;
}

export function canProceedToNext(): boolean {
  const currentState = state;
  
  switch (currentState.currentStep) {
    case 'language':
      return !!currentState.sourceLanguage;
    case 'files':
      return currentState.sourceFiles.length > 0;
    case 'mapping':
      // Mapping is optional
      return true;
    case 'summary':
      return !currentState.isStarting;
    default:
      return false;
  }
}

// Custom hook for wizard state
export function useWizardState(): WizardState {
  const [currentState, setCurrentState] = React.useState<WizardState>(getWizardState);
  
  React.useEffect(() => {
    return subscribeToWizard(() => {
      setCurrentState(getWizardState());
    });
  }, []);
  
  return currentState;
}
