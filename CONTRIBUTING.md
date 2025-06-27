# Contributing to flowfunc

First off, thank you for considering contributing to `flowfunc`! This document provides guidelines for contributing to the project.

## How Can I Contribute?

There are many ways to contribute, from writing code and documentation to submitting bug reports and feature requests.

### Reporting Bugs

If you find a bug, please ensure the bug was not already reported by searching on GitHub under [Issues](https://github.com/jmsnll/flowfunc/issues). If you're unable to find an open issue addressing the problem, open a new one. Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample or an executable test case** demonstrating the expected behavior that is not occurring.

### Suggesting Enhancements

If you have an idea for an enhancement, please first search the [Issues](https://github.com/jmsnll/flowfunc/issues) to see if it has been discussed before. If not, open a new issue describing the enhancement. Please provide:

* **A clear and descriptive title.**
* **A step-by-step description of the proposed enhancement** in as many details as possible.
* **The "why":** Explain the use case and reasoning behind the enhancement. Why would this be useful?

## Development Setup

Ready to contribute code? Here’s how to set up `flowfunc` for local development.

**1. Fork the repository:**

   First, fork the repository to your own GitHub account.

**2. Clone your fork:**

   Clone your forked repository to your local machine.

   ```bash
   git clone [https://github.com/YOUR_USERNAME/flowfunc.git](https://github.com/YOUR_USERNAME/flowfunc.git)
   cd flowfunc
   ```

**3. Create a virtual environment:**

   It's highly recommended to work in a virtual environment.

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   # On Windows, use: .venv\Scripts\activate
   ```

**4. Install dependencies:**

   This project uses `uv` for fast dependency management. The `[dev]` extra includes all tools needed for testing and linting.

   ```bash
   uv pip install -e ".[dev]"
   ```

**5. Set up pre-commit hooks:**

   This project uses `pre-commit` to ensure code style and quality checks are run before you commit.

   ```bash
   pre-commit install
   ```

You are now ready to make your changes!

## Making Changes

**1. Create a new branch:**

   Create a new branch from `main` for your changes. Use a descriptive name.

   ```bash
   # Example for a new feature
   git checkout -b feature/my-awesome-feature

   # Example for a bug fix
   git checkout -b fix/resolve-that-bug
   ```

**2. Write your code:**

   Make your changes in the codebase.

**3. Run tests and checks:**

   Before submitting your contribution, please ensure that the tests and quality checks pass.

   * **Run the test suite:**
      ```bash
      pytest
      ```

   * **Run the linter and formatter:** The pre-commit hooks will do this automatically, but you can run them manually:
      ```bash
      # Check formatting
      ruff format --check .

      # Check for linting errors
      ruff check .
      ```

   * **Run the type checker:**
      ```bash
      mypy .
      ```

## Submitting Your Contribution

1.  **Commit your changes:**
    Use a clear and descriptive commit message.

    ```bash
    git add .
    git commit -m "feat: Add my awesome new feature"
    ```

2.  **Push to your fork:**

    ```bash
    git push origin feature/my-awesome-feature
    ```

3.  **Open a Pull Request:**

    Open a pull request from your forked repository to the `main` branch of the original `flowfunc` repository.

    In the pull request description, please:
    * Clearly describe the problem and solution.
    * Reference the relevant issue number (e.g., `Closes #123`).
    * Include screenshots or code samples if applicable.

Once your pull request is submitted, a project maintainer will review it. We appreciate your patience and will provide feedback as soon as possible.

Thank you again for your contribution!