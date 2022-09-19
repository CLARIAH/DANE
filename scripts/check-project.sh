#!/bin/sh

# copy this script to .git/hooks/pre-commit

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd "$SCRIPTPATH"

poetry shell

cd ../

# run tests (configured in pyproject.toml)
pytest

# check lint rules (configured in .flake8)
flake8

# check formatting (configured in pyproject.toml)
black --check .

# check type annotations (configured in pyproject.toml)
mypy .
