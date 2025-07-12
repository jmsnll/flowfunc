# Broadcast Example

This example demonstrates how to use the `broadcast` map mode in flowfunc to run the same operation across multiple combinations of parameters.

## Overview

The workflow checks service health across multiple environments and regions simultaneously. It uses the broadcast mode to create a grid of all possible combinations of environments and regions.

## Files

- `workflow.yaml` - The workflow definition
- `health_checker.py` - The Python module containing the health check logic

## Setup

1. Install dependencies (from the project root):
   ```bash
   uv sync
   ```

2. Run the workflow (always from the project root):
   ```bash
   uv run flowfunc run examples/broadcast/workflow.yaml
   ```

> **Note:** Always run from the project root so Python can find the `examples` module. Running from inside the example folder will cause import errors.

## How it Works

The workflow defines two parameter lists:
- `environments`: ["playground", "staging", "production"]
- `regions`: ["us-east-1", "eu-west-1", "ap-southeast-2"]

With `map_mode: broadcast`, the `check_service_health` step will run 9 times (3 environments × 3 regions), creating a grid of health checks.

## Output

The workflow produces a `health_check_summary.json` file containing health status for all environment-region combinations. 