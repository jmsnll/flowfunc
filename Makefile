NAME := flowfunc

# UV command, ensuring it's available
UV := $(shell command -v uv 2> /dev/null)
UVX := $(shell command -v uvx 2> /dev/null)
VENV_DIR := .venv

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo ""
	@echo "  install     create virtualenv (if needed) and sync dependencies using uv"
	@echo "  clean       remove temporary files and the virtual environment"
	@echo "  lint        run code linters"
	@echo "  format      reformat code using Ruff"
	@echo "  test        run all tests using Pytest"
	@echo ""
	@echo "Check the Makefile to know exactly what each target is doing."


.PHONY: clean
clean:
	@echo ">>> Removing temporary files and caches..."
	find . -type d -name "__pycache__" -print0 | xargs -0 rm -rf --
	rm -rf .mypy_cache .ruff_cache .pytest_cache
	@echo ">>> Removing virtual environment '$(VENV_DIR)'..."
	rm -rf $(VENV_DIR) # uv venv is fast, so clean removal is often fine

.PHONY: lint
lint:
	@if [ -z $(UVX) ]; then echo "uvx could not be found."; exit 2; fi
	@echo ">>> Running Ruff linter..."
	$(UVX) ruff check ./tests/ ./examples/ ./$(NAME) --fix
	@echo ">>> Running Ruff formatter..."
	$(UVX) ruff format --check ./tests/ ./examples/ ./$(NAME) --fix

.PHONY: format
format:
	@if [ -z $(UV) ]; then echo "uv could not be found."; exit 2; fi
	@echo ">>> Formatting with Ruff format..."
	$(UVX) ruff format ./tests/ ./examples/ ./$(NAME)
	@echo ">>> Formatting with Ruff format..."
	$(UVX) isort ./tests/ ./examples/ ./$(NAME)
	@echo ">>> Applying auto-fixes with Ruff check --fix..."
	$(UVX) ruff check --fix-only ./tests/ ./examples/ ./$(NAME) --exit-non-zero-on-fix || true

.PHONY: test
test:
	@if [ -z $(UV) ]; then echo "uv could not be found."; exit 2; fi
	@echo ">>> Running tests with Pytest..."
	# TODO: Adjust --cov-fail-under percentage as appropriate for your project
	$(UV) run pytest ./tests/ --cov-report term-missing --cov-fail-under=90 --cov=./$(NAME)