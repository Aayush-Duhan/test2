import { LanguageDescription } from '@codemirror/language';

export const supportedLanguages = [
  LanguageDescription.of({
    name: 'SQL',
    extensions: ['sql'],
    async load() {
      const { sql, PostgreSQL } = await import('@codemirror/lang-sql');

      return sql({
        dialect: PostgreSQL,
        upperCaseKeywords: true,
      });
    },
  }),
  LanguageDescription.of({
    name: 'Markdown',
    extensions: ['md'],
    async load() {
      return import('@codemirror/lang-markdown').then((module) => module.markdown());
    },
  }),
];

export async function getLanguage(fileName: string) {
  const languageDescription = LanguageDescription.matchFilename(supportedLanguages, fileName);

  if (languageDescription) {
    return await languageDescription.load();
  }

  return undefined;
}