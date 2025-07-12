# Flowfunc Examples

This directory contains examples demonstrating different features and use cases of flowfunc.

## Examples Overview

### [Broadcast Example](./broadcast/)
Demonstrates the `broadcast` map mode to run operations across multiple parameter combinations. Creates a grid of health checks across environments and regions.

### [Pokemon Analysis Example](./pokemon_analysis/)
A multi-step pipeline that fetches, processes, and analyzes Pokémon data from the PokéAPI. Shows parallel processing and data aggregation.

### [Zip Example](./zip/)
Shows how to use the `zip` map mode to combine multiple lists element-wise. Creates character profiles by zipping names, archetypes, power levels, and creation years.

### [NLP Text Summarization Example](./nlp_text_summarization/)
A comprehensive NLP pipeline demonstrating text processing through multiple stages: tokenization, keyword extraction, summarization, and sentiment analysis.

## Running Examples

All examples use UV for dependency management. **Always run the workflow from the project root** so Python can find the `examples` module:

1. Install dependencies (from the project root):
   ```bash
   uv sync
   ```

2. Run the workflow (from the project root):
   ```bash
   uv run flowfunc run examples/[example-name]/workflow.yaml
   ```

> **Note:** Running from inside the example folder will cause import errors because Python will not find the `examples` module.

## Prerequisites

- [UV](https://docs.astral.sh/uv/) installed
- Python 3.8+ (specified in `.python-version` files)

## Example Features Demonstrated

- **Map Modes**: `broadcast`, `zip`, and default parallel processing
- **Multi-step Pipelines**: Sequential and parallel step execution
- **Data Processing**: API calls, text analysis, data transformation
- **Artifact Generation**: JSON output files with processed results
- **Parameter Configuration**: Workflow-level parameters and step inputs

Each example includes detailed documentation in its respective README file. 