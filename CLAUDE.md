# SDR Candidate Sourcer - Claude Agent Guide

## Overview
This is a Utah-focused SDR/AE candidate sourcing tool for Workstream. It uses Google X-Ray searches to find LinkedIn profiles of potential sales candidates, filters them based on role fit, and uploads them to Google Sheets.

## Available Commands

### Source Candidates
```bash
# Source 10 SDR candidates
python sdr_candidate_sourcer.py --count 10 --type sdr

# Source 5 AE candidates
python sdr_candidate_sourcer.py --count 5 --type ae

# Source both SDR and AE candidates
python sdr_candidate_sourcer.py --count 15 --type both

# Run a custom search query
python sdr_candidate_sourcer.py --query 'site:linkedin.com/in "BYU" "2024" Sales Utah'
```

### View Statistics
```bash
# Show current sourcing statistics without running searches
python sdr_candidate_sourcer.py --stats
```

### Dry Run (Preview)
```bash
# Preview what queries would be run without executing
python sdr_candidate_sourcer.py --dry-run
python sdr_candidate_sourcer.py --dry-run --type sdr
python sdr_candidate_sourcer.py --dry-run --count 5
```

### Update Experience Estimates
```bash
# Estimate and update Years of Experience column in Google Sheets
python update_experience.py
```

### Run Specific Batches
```bash
# Run batch 1 (first 8 queries)
python sdr_candidate_sourcer.py 1

# Run batch 2 (next 8 queries)
python sdr_candidate_sourcer.py 2
```

## Natural Language Commands

When users ask you to perform tasks, translate them to the appropriate commands:

| User Says | Command to Run |
|-----------|----------------|
| "Source 10 more SDR candidates" | `python sdr_candidate_sourcer.py --count 10 --type sdr` |
| "Find 5 AE candidates" | `python sdr_candidate_sourcer.py --count 5 --type ae` |
| "Show me stats" | `python sdr_candidate_sourcer.py --stats` |
| "What's our current pipeline?" | `python sdr_candidate_sourcer.py --stats` |
| "Update experience estimates" | `python update_experience.py` |
| "Run a dry run for SDRs" | `python sdr_candidate_sourcer.py --dry-run --type sdr` |
| "Source candidates" (no count) | `python sdr_candidate_sourcer.py --count 10 --type both` |

## Configuration

### API Keys
- **SerpAPI Key**: Set via `SERPAPI_KEY` environment variable or hardcoded in script
- **Google Sheets**: Requires service account credentials JSON file

### Google Sheets
- **Sheet ID**: `16VwCMk5pbInX7_YHMTRSaJp2caBd8v4GK-td6jux5JE`
- **Credentials File**: `sales-sourcing-6ef512645e0f.json`
- **Sheet Name**: `candidates`

### Output Columns
1. Full Name
2. LinkedIn URL
3. Headline
4. Years of Experience
5. Role Fit (SDR/AE/SDR-AE)
6. Notes
7. Email
8. Phone
9. Date Added
10. Status
11. AI Draft

## Workflow

1. **Search Phase**: Runs X-Ray Google searches via SerpAPI to find LinkedIn profiles
2. **Parsing Phase**: Extracts name, headline, and URL from search results
3. **Filtering Phase**:
   - Filters out senior executives (VP, Director, etc.)
   - Filters out existing SDR/BDRs (we want fresh candidates)
   - Filters out non-Utah candidates
4. **Role Classification**: Determines SDR vs AE fit based on headline keywords
5. **Upload Phase**: Real-time upload to Google Sheets
6. **Local Backup**: Saves to `candidates.csv`

## Query Types

### SDR Queries (fresh graduates, career pivoters)
- Recent college graduates (Class of 2023-2025)
- Student athletes (NCAA, Varsity)
- D2D sales experience (Vivint, solar, pest control)
- Restaurant/hospitality pivoters
- Entry-level positions

### AE Queries (experienced SaaS sellers)
- Utah tech companies (Qualtrics, Pluralsight, Podium, etc.)
- Account Executives with 2+ years SaaS experience
- SDRs ready for promotion to AE

## Memory File
Session history, statistics, and notes are tracked in `.claude/memory.md`. Update this file after each sourcing session with:
- Commands run
- Candidates found
- Any quality observations

## Agent Changelog
Changes to the agent itself (new commands, CLI args, configuration) are tracked in `.claude/agent_changelog.md`. Update this file when:
- Adding new commands or CLI arguments
- Modifying agent behavior
- Updating CLAUDE.md instructions
- Changing configuration

## Rate Limits
- SerpAPI: 100 free searches/month
- Delay between queries: 2-4 seconds
- Batch pause: 10 seconds every 8 queries
