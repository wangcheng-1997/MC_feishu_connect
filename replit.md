# MC Feishu Connect

A data connector that synchronizes data from Alibaba Cloud MaxCompute and SQL Server into Feishu (Lark) Base multi-dimensional tables.

## Architecture

**Monorepo** using NPM workspaces:

- `packages/frontend/` — React 18 + TypeScript + Ant Design UI, built with Vite
- `packages/backend/` — Node.js + Express API server

## Tech Stack

- **Frontend**: React 18, TypeScript, Ant Design, Vite, `@lark-base-open/connector-api`
- **Backend**: Node.js, Express, `mssql`, `odbc`, `axios`

## Running

The workflow command is `npm run deploy`, which:
1. Builds the frontend (`vite build`)
2. Starts the backend which serves the built frontend statically

Backend listens on port **5000** (or `$PORT`).

## Key Files

- `packages/backend/index.js` — Main Express server with all API routes
- `packages/backend/maxcompute_adapter.js` — MaxCompute data source adapter
- `packages/backend/sqlserver_adapter.js` — SQL Server data source adapter
- `packages/backend/sqlserver_handler.js` — SQL Server request handler
- `packages/backend/table_meta.js` — Table metadata retrieval
- `packages/backend/table_records.js` — Table records retrieval
- `packages/backend/request_sign.js` — Signature verification
- `packages/frontend/src/App.tsx` — Main configuration UI

## API Endpoints

- `GET /health` — Health check
- `POST /api/table_meta` — Fetch table schema/fields from MaxCompute
- `POST /api/records` — Fetch records from MaxCompute
- `POST /api/sqlserver/table_meta` — Fetch SQL Server table schema
- `POST /api/sqlserver/records` — Fetch SQL Server records
- `POST /api/sqlserver/tables` — List SQL Server tables

## Notes

- The frontend is a Feishu connector and is designed to run inside the Feishu Base environment
- The UI will appear blank outside the Feishu context as it relies on the Lark SDK
- TypeScript compilation step (`tsc -b`) was removed from build script since vite.config is `.js`
