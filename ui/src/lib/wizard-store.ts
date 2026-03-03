import React from 'react';

// Source languages supported by the migration tool
export const SOURCE_LANGUAGES = [
  { id: 'SqlServer', label: 'SQL Server' },
  { id: 'Redshift', label: 'Amazon Redshift' },
  { id: 'Oracle', label: 'Oracle' },
  { id: 'Teradata', label: 'Teradata' },
  { id: 'Synapse', label: 'Azure Synapse' },
  { id: 'BigQuery', label: 'Google BigQuery' },
  { id: 'Databricks', label: 'Databricks SQL' },
  { id: 'Greenplum', label: 'Greenplum' },
  { id: 'Sybase', label: 'Sybase IQ' },
  { id: 'Postgresql', label: 'PostgreSQL' },
  { id: 'Netezza', label: 'IBM Netezza' },
  { id: 'Spark', label: 'Spark SQL' },
  { id: 'Vertica', label: 'Vertica' },
  { id: 'Hive', label: 'Apache Hive' },
  { id: 'Db2', label: 'IBM DB2' },
] as const;

export type SourceLanguage = (typeof SOURCE_LANGUAGES)[number]['id'];

export const SCRIPT_TYPES = [
  'Tables',
  'Views',
  'Stored Procedures',
  'Functions',
  'Packages',
  'BTEQ',
  'MLOAD',
  'TPUMP',
] as const;

export type ScriptType = (typeof SCRIPT_TYPES)[number];

export const SUPPORTED_SCRIPT_TYPES: Record<SourceLanguage, ScriptType[]> = {
  Teradata: ['Tables', 'Views', 'Stored Procedures', 'Functions', 'BTEQ', 'MLOAD', 'TPUMP'],
  Oracle: ['Tables', 'Views', 'Stored Procedures', 'Functions', 'Packages'],
  SqlServer: ['Tables', 'Views', 'Stored Procedures', 'Functions'],
  Redshift: ['Tables', 'Views', 'Stored Procedures', 'Functions'],
  Synapse: ['Tables', 'Views', 'Stored Procedures', 'Functions'],
  Sybase: ['Tables', 'Views', 'Stored Procedures', 'Functions'],
  BigQuery: ['Tables', 'Views'],
  Greenplum: ['Tables', 'Views'],
  Netezza: ['Tables', 'Views'],
  Postgresql: ['Tables', 'Views'],
  Spark: ['Tables', 'Views'],
  Databricks: ['Tables', 'Views'],
  Vertica: ['Tables', 'Views'],
  Hive: ['Tables', 'Views'],
  Db2: ['Tables', 'Views', 'Stored Procedures', 'Functions'],
};

// Wizard steps
export const WIZARD_STEPS = [
  { id: 'language', label: 'Source Language', description: 'Select your source database' },
  { id: 'scriptType', label: 'Script Type', description: 'Select script types to convert' },
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
  sourceLanguage: SourceLanguage | '';

  // Step 2: Script Type
  scriptTypes: ScriptType[];
  
  // Step 3: Files
  sourceFiles: WizardFile[];
  
  // Step 4: Schema Mapping
  mappingFiles: WizardFile[];
  
  // Step 5: Summary
  isStarting: boolean;
  startError?: string;
}

// Initial state
const initialState: WizardState = {
  currentStep: 'language',
  completedSteps: [],
  sourceLanguage: '',
  scriptTypes: [],
  sourceFiles: [],
  mappingFiles: [],
  isStarting: false,
};

function getVisibleStepsForState(wizard: WizardState): readonly (typeof WIZARD_STEPS)[number][] {
  if (!wizard.sourceLanguage) {
    return WIZARD_STEPS.filter((step) => step.id !== 'scriptType');
  }
  return WIZARD_STEPS;
}

export function getVisibleWizardSteps() {
  return getVisibleStepsForState(state);
}

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
  const supported = SUPPORTED_SCRIPT_TYPES[language] ?? [];
  const nextScriptTypes = state.scriptTypes.filter((type) => supported.includes(type));
  state = { ...state, sourceLanguage: language, scriptTypes: nextScriptTypes };
  notifyListeners();
}

export function setScriptTypes(scriptTypes: ScriptType[]) {
  const supported = state.sourceLanguage ? SUPPORTED_SCRIPT_TYPES[state.sourceLanguage] ?? [] : [];
  const nextScriptTypes = scriptTypes.filter((type) => supported.includes(type));
  state = { ...state, scriptTypes: nextScriptTypes };
  notifyListeners();
}

export function toggleScriptType(scriptType: ScriptType) {
  const supported = state.sourceLanguage ? SUPPORTED_SCRIPT_TYPES[state.sourceLanguage] ?? [] : [];
  if (!supported.includes(scriptType)) {
    return;
  }

  const current = state.scriptTypes;
  if (current.includes(scriptType)) {
    state = { ...state, scriptTypes: current.filter((item) => item !== scriptType) };
  } else {
    state = { ...state, scriptTypes: [...current, scriptType] };
  }
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
  const visibleSteps = getVisibleStepsForState(state);
  const currentIndex = visibleSteps.findIndex(s => s.id === currentStep);
  
  // Mark current step as completed
  if (!completedSteps.includes(currentStep)) {
    state = { ...state, completedSteps: [...completedSteps, currentStep] };
  }
  
  // Go to next step
  if (currentIndex < visibleSteps.length - 1) {
    state = { ...state, currentStep: visibleSteps[currentIndex + 1].id };
  }
  notifyListeners();
}

export function goToPreviousStep() {
  const { currentStep } = state;
  const visibleSteps = getVisibleStepsForState(state);
  const currentIndex = visibleSteps.findIndex(s => s.id === currentStep);
  
  if (currentIndex > 0) {
    state = { ...state, currentStep: visibleSteps[currentIndex - 1].id };
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
  return getVisibleStepsForState(state).findIndex(s => s.id === currentStep);
}

export function isFirstStep(): boolean {
  return getCurrentStepIndex() === 0;
}

export function isLastStep(): boolean {
  return getCurrentStepIndex() === getVisibleStepsForState(state).length - 1;
}

export function canProceedToNext(): boolean {
  const currentState = state;
  
  switch (currentState.currentStep) {
    case 'language':
      return !!currentState.sourceLanguage;
    case 'scriptType':
      return currentState.scriptTypes.length > 0;
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
