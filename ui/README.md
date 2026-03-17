This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Python Execution Service

This UI expects a local Python execution service for real migration runs.

Start service (from repo root `C:/DB_MIG/TOOLS_SETUP/V8`):

```bash
uvicorn python_execution_service.main:app --host 127.0.0.1 --port 8090
```

Recommended environment variables:

```bash
EXECUTION_TOKEN=local-dev-token
PYTHON_EXECUTION_URL=http://127.0.0.1:8090
```

## GitHub Enterprise Import (PAT + SSO)

The setup wizard can import source files and schema mappings directly from GitHub Enterprise repositories. Users authenticate with a **Personal Access Token (classic)** that has `repo` scope.

### SSO / SAML

If your GitHub organization enforces SAML single sign-on, each user must **authorize their PAT for SSO** before it can access that org's repositories. When the API detects an unauthorized token, the UI surfaces a direct link to the GitHub SSO authorization page. See [Authorizing a PAT for SSO](https://docs.github.com/en/enterprise-cloud@latest/authentication/authenticating-with-single-sign-on/authorizing-a-personal-access-token-for-use-with-single-sign-on) for details.

### Token persistence

PATs and the selected organization name are stored in the browser's `sessionStorage` so users do not have to re-enter them each time the import modal opens. The token is cleared when the browser tab closes, or the user can click **Disconnect** inside the modal to clear it manually.

### Server-side configuration

No server-side secrets are required. The PAT is sent per-request from the browser to the Next.js API routes (`/api/github/repos`, `/api/github/branches`, `/api/github/tree`, `/api/github/files`), which forward it to the GitHub API.

If your GitHub Enterprise Server uses a custom API base URL (not `https://api.github.com`), set the following in `.env`:

```bash
GITHUB_ENTERPRISE_API_BASE_URL=https://github.example.com/api/v3
```

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
