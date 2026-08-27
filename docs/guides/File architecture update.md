# File Architecture Update

This document outlines the reorganization of the `my-ailounge` repository to follow a simplified folder structure.

## Step-by-Step Execution Plan

### Phase 1: Directory Creation
1. **Initialize Folders:**
   - `/docs/` (Task overviews, research, and usage guides)
   - `/diagrams/` (Mermaid charts and system architecture visuals)
   - `/src/` (Source code, bot files, and AI tools)
   - `/ideas/` (Brainstorming and future feature requests)
   - `/shawn/` (Personal scratchpads and task lists)

### Phase 2: Documentation & Migration
2. **Move Documentation & Research to `/docs/`:**
   - `RnD_Technical_Tasks_Overview.md`
   - `research.md`
   - `WARP.md`
   - `pricing.md`
   - `oh-my-opencode-models.md`
   - `reference-prompts.md`
   - `File architecture update.md` (this file)
3. **Move Visuals & Architecture to `/diagrams/`:**
   - `System Architecture mermaid chart.md`
   - `ranks.png`
4. **Move Source Code to `/src/`:**
   - `scraper.py`
5. **Move Brainstorming & Ideas to `/ideas/`:**
    - `ideas/trending.md`
   - `to-discuss.md`
6. **Move Personal Scratchpads to `/shawn/`:**
   - `Shawn_TO-DO.md`
   - `Shawn_zoho-research.md`
   - `zoho-research.md`

### Phase 3: Root Preservation
7. **Retain in Root:**
   - `.gitignore`
   - `.github/`
   - `.git/`
   - `README.md`

### Phase 4: Steer File Update
8. **Rewrite `README.md`:**
   - Update all internal links to reflect new nested paths.
   - Document the new project structure.
