import { Compartment, type Extension } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import { HighlightStyle, syntaxHighlighting } from '@codemirror/language';
import { tags } from '@lezer/highlight';

export interface EditorSettings {
  fontSize?: string;
  gutterFontSize?: string;
  tabSize?: number;
}

export const darkTheme = EditorView.theme({}, { dark: true });
export const themeSelection = new Compartment();

export function getTheme(settings: EditorSettings = {}): Extension {
  return [
    getEditorTheme(settings),
    themeSelection.of([getDarkTheme()]),
  ];
}

export function reconfigureTheme() {
  return themeSelection.reconfigure(getDarkTheme());
}

const syntaxTheme = HighlightStyle.define([
  { tag: [tags.keyword, tags.modifier], color: '#569cd6' },
  { tag: [tags.operator, tags.punctuation], color: '#d4d4d4' },
  { tag: [tags.string, tags.special(tags.string)], color: '#ce9178' },
  { tag: [tags.number, tags.integer, tags.float, tags.bool], color: '#b5cea8' },
  { tag: [tags.comment, tags.lineComment, tags.blockComment], color: '#6a9955', fontStyle: 'italic' },
  { tag: [tags.variableName], color: '#9cdcfe' },
  { tag: [tags.definition(tags.variableName)], color: '#4fc1ff' },
  { tag: [tags.typeName, tags.className, tags.namespace], color: '#4ec9b0' },
  { tag: [tags.function(tags.variableName), tags.labelName], color: '#dcdcaa' },
  { tag: [tags.propertyName], color: '#9cdcfe' },
  { tag: [tags.attributeName], color: '#c586c0' },
  { tag: [tags.heading], color: '#569cd6', fontWeight: 'bold' },
  { tag: [tags.emphasis], fontStyle: 'italic' },
  { tag: [tags.strong], fontWeight: 'bold' },
  { tag: [tags.link], color: '#4fc1ff', textDecoration: 'underline' },
]);

export function getSyntaxTheme(): Extension {
  return syntaxHighlighting(syntaxTheme);
}

function getEditorTheme(settings: EditorSettings) {
  return EditorView.theme({
    '&': {
      fontSize: settings.fontSize ?? '12px',
    },
    '&.cm-editor': {
      height: '100%',
      background: '#1e1e1e',
      color: '#d4d4d4',
    },
    '.cm-cursor': {
      borderLeft: '2px solid #aeafad',
    },
    '.cm-scroller': {
      lineHeight: '1.5',
      '&:focus-visible': {
        outline: 'none',
      },
    },
    '.cm-line': {
      padding: '0 0 0 4px',
    },
    '&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground': {
      backgroundColor: '#264f78 !important',
      opacity: '0.3',
    },
    '&:not(.cm-focused) > .cm-scroller > .cm-selectionLayer .cm-selectionBackground': {
      backgroundColor: '#3a3d41',
      opacity: '0.3',
    },
    '&.cm-focused > .cm-scroller .cm-matchingBracket': {
      backgroundColor: '#515a6b',
    },
    '.cm-activeLine': {
      background: '#282828',
    },
    '.cm-gutters': {
      background: '#1e1e1e',
      borderRight: 0,
      color: '#858585',
    },
    '.cm-gutter': {
      '&.cm-lineNumbers': {
        fontFamily: 'Roboto Mono, monospace',
        fontSize: settings.gutterFontSize ?? settings.fontSize ?? '12px',
        minWidth: '40px',
      },
      '& .cm-activeLineGutter': {
        background: 'transparent',
        color: '#c6c6c6',
      },
      '&.cm-foldGutter .cm-gutterElement > .fold-icon': {
        cursor: 'pointer',
        color: '#c5c5c5',
        transform: 'translateY(2px)',
        '&:hover': {
          color: '#ffffff',
        },
      },
    },
    '.cm-foldGutter .cm-gutterElement': {
      padding: '0 4px',
    },
    '.cm-tooltip-autocomplete > ul > li': {
      minHeight: '18px',
    },
    '.cm-panels': {
      borderColor: '#3c3c3c',
    },
    '.cm-panels-bottom': {
      borderTop: '1px solid #3c3c3c',
      backgroundColor: 'transparent',
    },
    '.cm-tooltip': {
      background: '#252526',
      border: '1px solid #454545',
      color: '#cccccc',
    },
    '.cm-tooltip.cm-tooltip-autocomplete ul li[aria-selected]': {
      background: '#094771',
      color: '#ffffff',
    },
    '.cm-searchMatch': {
      backgroundColor: '#613214',
    },
    // Syntax highlighting colors
    '.cm-content': {
      caretColor: '#aeafad',
    },
    '&.cm-focused .cm-cursor': {
      borderLeftColor: '#aeafad',
    },
    // Token colors for SQL and Markdown
    '.tok-keyword': {
      color: '#569cd6',
    },
    '.tok-operator': {
      color: '#d4d4d4',
    },
    '.tok-string': {
      color: '#ce9178',
    },
    '.tok-number': {
      color: '#b5cea8',
    },
    '.tok-comment': {
      color: '#6a9955',
    },
    '.tok-variableName': {
      color: '#9cdcfe',
    },
    '.tok-typeName': {
      color: '#4ec9b0',
    },
    '.tok-punctuation': {
      color: '#d4d4d4',
    },
    '.tok-bracket': {
      color: '#ffd700',
    },
    '.tok-heading': {
      color: '#569cd6',
      fontWeight: 'bold',
    },
    '.tok-strong': {
      fontWeight: 'bold',
    },
    '.tok-emphasis': {
      fontStyle: 'italic',
    },
    '.tok-link': {
      color: '#ce9178',
      textDecoration: 'underline',
    },
    '.tok-monospace': {
      fontFamily: 'monospace',
      color: '#ce9178',
    },
  });
}

function getDarkTheme(): Extension {
  return EditorView.theme({}, { dark: true });
}
