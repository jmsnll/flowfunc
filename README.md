# flowfunc

[![PyPI version](https://badge.fury.io/py/flowfunc.svg)](https://badge.fury.io/py/flowfunc)
[![CI](https://github.com/your-username/flowfunc/actions/workflows/ci.yaml/badge.svg)](https://github.com/your-username/flowfunc/actions/workflows/ci.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**flowfunc: The Dead Simple, Local-First Workflow Runner for Python.**

---

`flowfunc` is for the developer, researcher, or tinkerer who needs more than a shell script but less than a production-grade orchestrator like Airflow or Dagster. It helps you define and run multi-step, reproducible workflows on your local machine with zero infrastructure overhead.

It's built on a few core principles:
* **Local-First:** Everything runs on your machine. No servers, no schedulers, no databases.
* **Zero Infrastructure:** If you have Python and `pip`, you have everything you need.
* **Python-Native:** Your workflow steps are just Python functions.
* **Simplicity Over Complexity:** `flowfunc` is designed to be obvious, not clever.

### Installation

```bash
pip install flowfunc
```

### Quickstart: Your First Workflow

Let's build a simple workflow that greets a user and saves the message to a file.

**1. Initialize a new project:**

This command creates a simple project structure for you.

```bash
flowfunc init my_first_workflow
cd my_first_workflow
```

This will generate two files: `main.py` for your Python logic and `workflow.yaml` to define the workflow.

**2. Write your Python functions (`main.py`):**

Your workflow steps are just plain Python functions.

```python
# main.py

def create_greeting(name: str) -> str:
    """Creates a greeting message."""
    print(f"Creating greeting for {name}...")
    return f"Hello, {name}!"

def save_message(message: str, output_path: str) -> str:
    """Saves the message to a text file."""
    print(f"Saving message to {output_path}...")
    with open(output_path, "w") as f:
        f.write(message)
    return output_path
```

**3. Define your workflow (`workflow.yaml`):**

This YAML file connects your Python functions into a directed acyclic graph (DAG).

```yaml
# workflow.yaml

apiVersion: flowfunc.dev/v1alpha1
kind: Workflow
metadata:
  name: hello-world-workflow
  description: "A simple workflow to greet a user and save the message."

spec:
  # Python module where your functions are defined
  module: main

  # Inputs for the entire workflow
  inputs:
    - name: user_name
      description: "The name of the person to greet."
      default: "World"
    - name: output_file
      default: "greeting.txt"

  # The steps of your workflow
  steps:
    - name: make_greeting
      # Calls the create_greeting function
      function: create_greeting
      # Maps workflow inputs to function arguments
      args:
        name: "{{ inputs.user_name }}"

    - name: write_to_file
      # Calls the save_message function
      function: save_message
      # The `message` argument comes from the output of the `make_greeting` step
      args:
        message: "{{ steps.make_greeting.outputs.return_value }}"
        output_path: "{{ inputs.output_file }}"
```

**4. Run the workflow!**

```bash
flowfunc run workflow.yaml
```

You can also override inputs from the command line:

```bash
flowfunc run workflow.yaml --input user_name=Alice --input output_file=alice.txt
```

A new file, `alice.txt`, will be created with the content "Hello, Alice!". A `.flowfunc` directory will also appear, containing metadata and a summary of your run.

### Key Commands

* `flowfunc run <file>`: Execute a workflow.
* `flowfunc init <dir>`: Create a new boilerplate project.
* `flowfunc new <file>`: Create a new boilerplate workflow file.
* `flowfunc graph <file>`: Display the workflow's dependency graph in the terminal.
* `flowfunc docs <file>`: View the workflow's documentation (inputs, outputs, descriptions) in the terminal.

### Why `flowfunc`?

Choose `flowfunc` when:
* Your project has grown beyond a single script.
* You need to reliably chain together multiple steps (e.g., download data -> process it -> generate a plot).
* You want to track inputs and outputs for reproducibility without a complex setup.
* You want a simple CLI to run your pipelines.

Avoid `flowfunc` when you need:
* Distributed execution across multiple machines.
* A centralized scheduler for time-based runs.
* A UI dashboard and enterprise features.

For those cases, consider mature tools like [Dagster](https://dagster.io/), [Prefect](https://www.prefect.io/), or [Argo Workflows](https://argoproj.github.io/argo-workflows/).

### Under the Hood

`flowfunc` provides the workflow management layer (schema, CLI, persistence, etc.) and uses the excellent [pipefunc](https://github.com/ML-Dev-Ops/pipefunc) library for the core DAG resolution and execution.

### Contributing

Contributions are welcome! Please read the `CONTRIBUTING.md` file for details on how to set up your development environment and submit a pull request.

### License

This project is licensed under the MIT License.