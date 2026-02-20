# Plan E: Integration & Polish (Final Session)

**Branch:** `main` (merge all feature branches)
**Estimated time:** 15-20 min

## Objective

Merge all sessions, wire the end-to-end CLI, write the README, and produce final output artifacts.

## Prerequisites

All four sessions (A-D) must be COMPLETE. Check COORDINATION.md.

## Tasks

### 1. Merge all branches into main

```bash
git checkout main && git pull
git merge feature/data-generator
git merge feature/pipeline
git merge feature/analytics
git merge feature/visualization
```

Resolve any conflicts (should be minimal since each session owns separate files).

### 2. End-to-End CLI (`src/main.py`)

Create a Click-based CLI that runs everything in sequence:

```bash
python -m src.main generate    # Run data generator
python -m src.main pipeline    # Run pipeline
python -m src.main analyze     # Run analytics
python -m src.main visualize   # Generate dashboard + charts
python -m src.main run-all     # Full end-to-end
```

### 3. Integration Testing

- Run `python -m src.main run-all` end-to-end
- Verify all output files exist
- Verify the dashboard shows the correct insights
- Check that the 3 embedded patterns are surfaced

### 4. README.md

Write a comprehensive README with:
- Project overview and architecture diagram
- Setup instructions (venv, install, run)
- Key findings summary (the 3-5 insights)
- Screenshot of the dashboard
- Design decisions and trade-offs

### 5. Final Output

- Run Streamlit dashboard, take screenshots
- Generate static HTML dashboard
- Ensure all output artifacts are in `output/`

### 6. Final commit & push

- Clean up any debug code
- Final commit to main
- Push everything
