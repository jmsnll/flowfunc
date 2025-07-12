# Advanced Example: GitHub OSS Project Health Analyzer

This example demonstrates a highly sophisticated multi-stage data processing pipeline that showcases advanced flowfunc capabilities for parallel data gathering, transformation, and aggregation.

## Overview

The OSS Health Analyzer is designed to stress-test flowfunc with a real-world scenario involving:

- **Complex workflow** with multiple parallel branches
- **Fan-out/Fan-in patterns** for processing lists of inputs and aggregating results
- **Advanced data transformation** with statistical analysis
- **Robust error handling** for failed API requests
- **Artifact generation** creating a comprehensive Markdown report

## Features Demonstrated

### 1. Complex Parallel Processing
The pipeline processes multiple GitHub repositories simultaneously across three parallel data fetching operations:
- Repository metadata (stars, forks, issues)
- Recent commit activity (last 25 commits)
- Open issues (last 15 non-PR issues)

### 2. Fan-out/Fan-in Pattern
- **Fan-out**: Takes a list of repository slugs and processes each one in parallel
- **Fan-in**: Aggregates all individual repository scores into a single comprehensive report

### 3. Advanced Map Modes
- **`map_mode: map`**: For parallel processing of individual repositories
- **`map_mode: zip`**: For combining disparate data sources (details, commit cadence, issue staleness) for each repository
- **`map_mode: none`**: For final aggregation of all processed entities into a single report

### 4. Data Transformation Pipeline
The workflow demonstrates sophisticated data processing:
1. **Raw Data Fetching**: API calls to GitHub
2. **Statistical Analysis**: Calculate commit cadence and issue staleness
3. **Scoring Algorithm**: Combine metrics into health scores
4. **Report Generation**: Create human-readable Markdown output

### 5. Graceful Error Handling
- Handles repositories that don't exist (404 errors)
- Continues processing even when some API calls fail
- Reports failed repositories separately in the final output

### 6. Artifact Generation
Creates a comprehensive Markdown report (`project_health_report.md`) containing:
- Repository health rankings
- Summary statistics
- Failed repository listings

## Pipeline Architecture

```
Input: List of Repository Slugs
    ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 1: Parallel Data Fetching                           │
│ ├── fetch_repo_details (map)                              │
│ ├── fetch_commit_activity (map)                           │
│ └── fetch_open_issues (map)                               │
└─────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 2: Parallel Data Analysis                           │
│ ├── analyze_commit_cadence (map)                          │
│ └── analyze_issue_staleness (map)                         │
└─────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 3: Parallel Scoring (zip mode)                      │
│ └── generate_health_score (zip)                           │
└─────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 4: Final Aggregation (none mode)                    │
│ └── create_markdown_report (none)                         │
└─────────────────────────────────────────────────────────────┘
    ↓
Output: project_health_report.md
```

## Health Scoring Algorithm

The health score combines multiple metrics:

1. **Popularity Score**: `(stars / 1000) + (forks / 500)`
2. **Activity Penalty**: Subtract average commit cadence (lower is better)
3. **Maintenance Penalty**: Subtract `(issue_staleness / 10)` (lower is better)
4. **Final Score**: Ensured to be non-negative

## How to Run

```bash
# Run the workflow with default repositories
flowfunc run examples/oss_health_analyzer/workflow.yaml

# Run with custom repository list
flowfunc run examples/oss_health_analyzer/workflow.yaml --param repo_slugs='["torvalds/linux", "microsoft/vscode", "facebook/react"]'
```

## Output

The workflow generates a `project_health_report.md` file in the artifacts directory containing:

- **Repository Health Rankings**: Sorted table of all analyzed repositories
- **Summary Statistics**: Average scores, total stars/forks, most active repo
- **Failed Repositories**: List of repositories that couldn't be analyzed

## Example Output Structure

```markdown
# OSS Health Analysis Report

**Analysis Date:** 2024-01-15 14:30:25 UTC
**Total Repositories Analyzed:** 5
**Successful:** 4
**Failed:** 1

## Repository Health Rankings

| Rank | Repository | Health Score | Stars | Forks | Avg Commit Cadence (Days) | Avg Issue Age (Days) | Open Issues |
|------|------------|--------------|-------|-------|---------------------------|---------------------|-------------|
| 1 | prefect | 45.2 | 15,234 | 1,456 | 2.3 | 15.7 | 234 |
| 2 | flowfunc | 32.1 | 1,234 | 89 | 1.8 | 8.2 | 45 |
...

## Summary Statistics

- **Average Health Score:** 28.45
- **Total Stars:** 18,456
- **Total Forks:** 1,789
- **Most Active Repository:** prefect (Score: 45.2)

## Failed Repositories

The following repositories could not be analyzed:
- **a-made-up-repo/that-will-fail**: Repo not found
```

## Dependencies

This example requires the `requests` library for HTTP API calls. The workflow uses the public GitHub API without authentication, so it's subject to rate limiting.

## Real-World Applications

This pattern is commonly used for:
- **Multi-tenant data processing**: Analyzing multiple customer datasets
- **Batch data validation**: Checking multiple data sources
- **Comparative analysis**: Evaluating multiple options or candidates
- **Health monitoring**: Checking multiple system components
- **Report generation**: Aggregating data from multiple sources 