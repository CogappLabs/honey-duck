# Frontend Dashboard

A Next.js dashboard for monitoring Dagster pipelines, providing a client-friendly view of pipeline status, runs, and assets.

## Quick Start

```bash
cd frontend
npm install
npm run dev
```

The dashboard runs at [http://localhost:3001](http://localhost:3001) and connects to Dagster at `http://127.0.0.1:3000`.

## Routes

| Route | Description |
|-------|-------------|
| `/` | Dashboard home with pipeline health overview |
| `/assets` | Browse all assets with group filtering |
| `/assets/{path}` | Asset detail with materialization history |
| `/runs` | Run history with status and duration |
| `/jobs` | Available pipeline jobs with launch links |

## Dashboard Features

### Homepage

- **Pipeline Health** - Overall status indicator (Healthy/Warning/Critical)
- **24h Stats** - Success, failed, and running run counts
- **Failures Alert** - Prominent display of recent failures with links to Dagster
- **Last Successful Run** - Timestamp of most recent successful execution
- **Recent Runs** - Table of latest runs with status, duration, and links

### Assets Page

- Filter assets by group using tab navigation
- View asset details including description and compute kind
- Click through to see materialization history

### Runs Page

- View last 50 runs with full details
- Status badges, timestamps, and duration
- Direct links to Dagster UI for each run

### Jobs Page

- List of available pipeline jobs
- Launch links that open directly in Dagster UI
- Internal Dagster jobs are filtered out

## Tech Stack

| Category | Technology |
|----------|------------|
| Framework | Next.js 16 (App Router) |
| Language | TypeScript |
| Styling | Tailwind CSS v4 |
| GraphQL | urql |
| Linting | Biome |

## Project Structure

```
frontend/src/
├── app/
│   ├── layout.tsx              # Root layout with navigation
│   ├── page.tsx                # Dashboard home
│   ├── providers.tsx           # urql GraphQL provider
│   ├── assets/
│   │   ├── page.tsx            # Assets list
│   │   └── [...path]/page.tsx  # Asset detail
│   ├── runs/page.tsx           # Runs history
│   ├── jobs/page.tsx           # Jobs list
│   └── api/graphql/route.ts    # GraphQL proxy
├── components/
│   └── StatusBadge.tsx         # Reusable status indicator
└── lib/
    ├── graphql.ts              # urql client setup
    ├── queries.ts              # GraphQL queries
    ├── types.ts                # Shared TypeScript types
    └── utils.ts                # Utility functions
```

## Configuration

### Environment Variables

Create a `.env.local` file in the `frontend/` directory:

```bash
# Dagster GraphQL endpoint (default: http://127.0.0.1:3000/graphql)
DAGSTER_GRAPHQL_URL=http://127.0.0.1:3000/graphql
```

### GraphQL Proxy

The dashboard proxies GraphQL requests through `/api/graphql` to avoid CORS issues. This route forwards requests to the Dagster GraphQL endpoint.

## Development

### Adding New Pages

1. Create a new directory under `src/app/`
2. Add a `page.tsx` file with your component
3. Use shared types from `@/lib/types`
4. Add GraphQL queries to `@/lib/queries.ts`

### Adding Components

Place reusable components in `src/components/`. Use arrow function syntax:

```tsx
export const MyComponent = ({ prop }: { prop: string }) => (
  <div>{prop}</div>
)
```

### GraphQL Queries

Queries are defined in `src/lib/queries.ts` using the `gql` template tag from urql:

```tsx
import { gql } from "urql"

export const MY_QUERY = gql`
  query MyQuery {
    # ...
  }
`
```

Use queries in components with the `useQuery` hook:

```tsx
import { useQuery } from "urql"
import { MY_QUERY } from "@/lib/queries"

const MyComponent = () => {
  const [result] = useQuery({ query: MY_QUERY })
  // ...
}
```
