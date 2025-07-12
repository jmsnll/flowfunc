# Pokemon Analysis Example

This example demonstrates a multi-step pipeline that fetches, processes, and analyzes Pokémon data from the PokéAPI.

## Overview

The workflow performs the following steps:
1. **Fetch Pokémon Data**: Retrieves raw data for multiple Pokémon IDs in parallel
2. **Extract Basic Stats**: Processes the raw data to extract key statistics
3. **Summarize Stats**: Aggregates all individual stats into a comprehensive summary

## Files

- `workflow.yaml` - The workflow definition
- `main.py` - The Python module containing all the processing logic

## Setup

1. Install dependencies (from the project root):
   ```bash
   uv sync
   ```

2. Run the workflow (always from the project root):
   ```bash
   uv run flowfunc run examples/pokemon_analysis/workflow.yaml
   ```

> **Note:** Always run from the project root so Python can find the `examples` module. Running from inside the example folder will cause import errors.

## How it Works

The workflow processes a list of Pokémon IDs (1-10 by default) through three stages:

1. **Parallel Fetching**: Each Pokémon ID is fetched independently using the default map mode
2. **Data Extraction**: Raw data is processed to extract key stats (name, height, weight, types, etc.)
3. **Summary Aggregation**: All individual stats are combined into a single summary report

## Configuration

You can modify the `pokemon_ids` parameter in the workflow to analyze different Pokémon:

```yaml
params:
  pokemon_ids:
    value: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
```

## Output

The workflow produces a `pokemon_summary.json` file containing aggregated statistics for all processed Pokémon. 