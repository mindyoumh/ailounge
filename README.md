# Mind You AI Council and AI Factory

This repository functions as the **Mind You AI Council and AI Factory**, an advanced, AI-powered internal ecosystem engineered to elevate organizational productivity by a factor of 100x. By centralizing strategic AI-driven management and operational support, it reduces manual workload, boosts efficiency, and enables seamless coordination across all technical domains.

---

## Table of Contents

- [Introduction](#introduction)
- [Developer Intelligence Feed](#developer-intelligence-feed)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Tooling](#tooling)
- [Pricing](#pricing)
- [Roles](#roles)
- [Research](#research)
- [Usage Guide](#usage-guide)
- [Contributing](#contributing)
- [FAQs and Support](#faqs-and-support)

---

## Introduction

Welcome to the Mind You AI ecosystem, where you can supercharge your workflows with tailored agent models, advanced tools, and best-in-class productivity software. This repository provides everything needed to implement AI in your organization with structured guidance, clear references, and adaptable technologies.

---

## New Team Members - Start Here

**Welcome to the team!** If you're a new employee or intern, start with our comprehensive onboarding guide:

**→ [Developer Onboarding Guide](./docs/getting-started/INSTRUCTIONS.md)**

This guide walks you through:

- Getting GitHub Copilot through GitHub Education
- Getting Gemini Pro through Google Education
- Setting up VS Code with GitHub Copilot
- Installing and configuring OpenCode CLI
- Installing and configuring Gemini CLI
- Complete verification and testing

**Estimated time:** 2–3 hours (including approval wait times)

---

## Developer Intelligence Feed

The Developer Intelligence Feed is an engineering intelligence dashboard currently being developed within the AI Factory ecosystem. Its purpose is to aggregate and centralize high-signal technical news, discussions, trends, security updates, engineering resources, and curated feed sources into a single searchable interface.

The platform currently supports ingestion from:

- Hacker News
- RSS Feeds
- GitHub Trending
- Manually Curated Feed Sources

All feed data is normalized and stored in a centralized Supabase PostgreSQL database, allowing the dashboard to provide a unified view of engineering-related information.

### Dashboard Architecture

```text
Feed Sources
(Manual, RSS, Hacker News, GitHub Trending)
        ↓
Feed Ingesters
        ↓
Supabase PostgreSQL
        ↓
API Layer (/api/feed)
        ↓
Engineering Briefing
Feed Dashboard
```

### Tech Stack

#### Frontend

- Next.js 16
- React 19
- TypeScript
- Tailwind CSS
- Radix UI

#### Backend

- Next.js Route Handlers
- TypeScript

#### Database

- Supabase PostgreSQL
- @supabase/supabase-js

#### Automation

- GitHub Actions
- Feed Ingestion Pipelines

---

### Dashboard Dependencies

The Developer Intelligence Feed dashboard is built using the following packages.

#### Core Framework

- next
- react
- react-dom
- typescript

#### UI Components

- @radix-ui/react-select
- @radix-ui/react-separator
- @radix-ui/react-slot
- @radix-ui/react-tabs
- @radix-ui/react-toggle

#### Styling

- tailwindcss
- @tailwindcss/postcss
- postcss
- autoprefixer
- class-variance-authority
- clsx
- tailwind-merge

#### Icons

- lucide-react

#### Development Tooling

- tsx
- ts-node
- @types/node
- @types/react
- @types/react-dom

---

### First-Time Setup

Clone the repository and install dependencies:

```bash
npm install
```

Set up Supabase PostgreSQL (requires `.env.local` with `NEXT_PUBLIC_SUPABASE_URL`,
`NEXT_PUBLIC_SUPABASE_ANON_KEY`, and `SUPABASE_SERVICE_ROLE_KEY`):

```bash
# Run docs/supabase-schema.sql in Supabase SQL editor first, then:
npm run db:migrate
```

Or set up the database:

```bash
npm run db:migrate
```

Run all feed ingesters:

```bash
npm run ingest
```

Start the dashboard:

```bash
npm run dev
```

Open:

```text
http://localhost:3000
```

Dashboard Feed:

```text
http://localhost:3000/feed
```

> Note: The project uses Supabase PostgreSQL (migrated from SQLite).

---

### Available Commands

#### Dashboard

```bash
npm run dev
npm run build
npm run start
```

#### Database

```bash
npm run db:migrate
```

#### Feed Ingestion

```bash
npm run ingest
npm run ingest:hn
npm run ingest:rss
npm run ingest:trending
npm run ingest:manual
```

---

### End-to-End Workflow

The Developer Intelligence Feed operates through a straightforward ingestion and visualization pipeline.

1. Run:

```bash
npm run ingest
```

2. The ingestion pipeline executes all configured ingesters:
   - Manual Feeds
   - RSS Feeds
   - Hacker News
   - GitHub Trending

3. Each ingester fetches new content and stores it in:

```text
Supabase PostgreSQL
```

4. Records are normalized and inserted into Supabase PostgreSQL using database constraints that automatically prevent duplicate entries.

5. The Next.js application reads directly from the same database.

6. The Engineering Briefing page loads statistics and recent feed items directly from Supabase PostgreSQL using Server Components.

7. The Feed Dashboard retrieves records through:

```text
/api/feed
```

8. Users can:
   - Search content
   - Filter by source
   - Filter by category
   - Browse paginated results
   - Mark items as read
   - Pin important items
   - Add manual entries
   - Delete entries

9. Refreshing the browser immediately reflects newly ingested content without requiring a rebuild or restart.

---

### Current Dashboard Features

- Engineering Briefing Dashboard
- Feed Aggregation
- Search
- Source Filtering
- Category Filtering
- Pagination
- Read/Unread Tracking
- Item Pinning
- Manual Feed Creation
- Feed Item Deletion
- Supabase PostgreSQL Persistence

---

### Future Direction

The project is currently evaluating alternatives to traditional cron-based scheduling for automated feed ingestion workflows.

Areas under investigation include:

- GitHub Actions automation
- Event-driven workflows
- Scheduled ingestion pipelines
- Lightweight orchestration mechanisms
- Alternative automation platforms

The goal is to reduce operational overhead while maintaining reliable feed updates.

---

## Key Features

- **AI Role Optimization**: Empower agents to handle communication triage, infrastructure hygiene, and knowledge management efficiently.
- **Reduced Overhead**: Streamline delivery management and system planning.
- **Research-Driven Choices**: Robust market analysis to identify cost-efficient, high-performance AI models.

---

## Project Structure

- `/app/` - Next.js application pages, dashboard UI, and API routes.
- `/components/` - Reusable React components and UI elements.
- `/src/` - Feed ingesters, database layer, and backend utilities.
- `/docs/` - Research, planning, feed definitions, and documentation.
- `/diagrams/` - Architecture diagrams and workflow visualizations.
- `/ideas/` - Brainstorming, experiments, and proposals.
- `/intern-logs/` - Contributor workspaces and task tracking.
- `/data/` - Legacy SQLite database storage (no longer actively used).

---

## Tooling

### Gemini CLI

- Designed for management, planning, summaries, and coordination tasks.
- Features model auto-selection and rate-limit awareness.

### OpenCode CLI

- Engineered for structured agent execution and software development workflows.

### Installed Skills

The team currently uses OpenCode-compatible skills to accelerate engineering work:

- planning-and-task-breakdown
- ui-ux-pro-max
- github-deep-research
- caveman

These skills assist with architecture planning, dashboard design, repository research, and concise technical reasoning.

For more details, see [Tooling Documentation](./docs/research/vibe-coding-vs-legacy.md).

---

## Pricing

Explore detailed pricing analysis models, benchmarks, and strategies in the [Pricing Documentation](./docs/research/pricing.md).

---

## Roles

The AI ecosystem leverages distinct roles that include:

1. Sisyphus (Orchestrator)
2. Oracle (Architect)
3. Librarian (Researcher)

For a complete breakdown, refer to the [Role Definitions](./docs/tooling/oh-my-opencode-models.md).

---

## Research

The ecosystem fosters a research-driven workflow. Access benchmarks, live leaderboards, and plugins via the [Research Resource](./docs/research/research.md).

---

## Usage Guide

### Getting Started

1. Read the Role Definitions documentation.
2. Complete the onboarding guide.
3. Install required tooling and AI agents.
4. Initialize the dashboard database.
5. Run feed ingestion.
6. Launch the dashboard locally.
7. Begin development.

---

## Contributing

We welcome contributions.

Recommended workflow:

1. Create a feature branch.
2. Implement changes.
3. Verify ingestion and dashboard functionality.
4. Submit a Pull Request.
5. Request review from the engineering team.

For additional discussions and proposals, see:

`ideas/to-discuss.md`

---

## FAQs and Support

Frequently asked questions and troubleshooting guidelines are available in:

`docs/tooling/WARP.md`

---

## Targets for 2026

### Engineering Goals

- Automate repetitive engineering workflows.
- Expand feed coverage.
- Improve developer intelligence capabilities.
- Evaluate automation alternatives to cron scheduling.

### Quality Improvements

- Enhance architecture reviews.
- Improve dashboard usability.
- Strengthen feed quality and relevance.

For complete targets, refer to:

`docs/tooling/reference-prompts.md`

---

This repository evolves continuously through the contributions of the engineering team, interns, and AI-assisted development workflows.
