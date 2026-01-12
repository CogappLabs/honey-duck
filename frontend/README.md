# Honey Duck Frontend

Next.js dashboard for the Honey Duck Dagster pipeline.

## Features

- View all pipeline assets grouped by category
- Monitor recent runs and their status
- Browse available jobs
- Links to Dagster UI for detailed views

## Setup

```bash
cd frontend
npm install
npm run dev
```

Open http://localhost:3001 in your browser.

**Note:** Dagster must be running at `http://127.0.0.1:3000` for the dashboard to fetch data.

## Stack

- Next.js 16 with App Router
- TypeScript
- Tailwind CSS
- urql (GraphQL client)
- Biome (linting/formatting)

## Scripts

```bash
npm run dev      # Start development server on port 3001
npm run build    # Build for production
npm run start    # Start production server
npm run lint     # Run Biome linter
npm run format   # Format code with Biome
npm run check    # Run Biome check with auto-fix
```

## Configuration

The GraphQL endpoint can be configured via environment variable:

```bash
NEXT_PUBLIC_DAGSTER_GRAPHQL_URL=http://127.0.0.1:3000/graphql
```
