---
title: Pre-Commit Hooks
description: Configure Lefthook pre-commit hooks for linting, formatting, and testing with ruff, ty, and pytest.
---

# Pre-Commit Hooks with Lefthook

Automated code quality checks that run before every commit to ensure code consistency and catch issues early.

## What Gets Checked

Every commit automatically runs:

1. **Ruff Linting** - Fast Python linter (auto-fixes issues)
2. **Ruff Formatting** - Code formatting check
3. **ty Type Checking** - Static type analysis (Astral's Rust-based checker)
4. **Pytest Tests** - Full test suite

**If any check fails, the commit is prevented** 

---

## Quick Start

### Install Hooks

```bash
# Option 1: Use setup script (recommended)
bash scripts/setup_hooks.sh

# Option 2: Manual install
uv sync  # Installs lefthook
lefthook install
```

**That's it!** Hooks are now active for all commits.

### Test the Hooks

```bash
# Make a change
echo "test" >> README.md

# Try to commit (hooks will run)
git add README.md
git commit -m "test commit"

# Hooks run automatically:
# → Ruff check
# → Ruff format
# → ty
# → Pytest
```

---

## Usage

### Normal Commits

Just commit as usual - hooks run automatically:

```bash
git add .
git commit -m "Add new feature"

# Hooks run:
# ✓ Ruff linting... passed
# ✓ Ruff formatting... passed
# ✓ ty type check... passed
# ✓ Pytest... passed
# Commit successful!
```

### Skip Hooks (Emergency Only)

Sometimes you need to commit without running hooks:

```bash
# Skip all hooks for this commit
LEFTHOOK=0 git commit -m "WIP: debugging"

# Or use git's --no-verify flag
git commit --no-verify -m "WIP: debugging"
```

** Warning**: Only skip hooks when absolutely necessary (e.g., WIP commits on feature branch). Never skip on main/master!

---

## What Each Check Does

### 1. Ruff Linting

**What**: Fast Python linter that checks code style and common errors
**Auto-fixes**: Yes (automatically fixes simple issues)
**Config**: `pyproject.toml` → `[tool.ruff]`

**Common issues caught**:
- Unused imports
- Undefined variables
- F-string issues
- Line too long
- Import sorting

**Example**:
```python
# Before Ruff
import os
import sys
import pandas  # Unused import

def foo( ):  # Extra spaces
    x=1+2  # Missing spaces
    return x

# After Ruff (auto-fixed)
import os
import sys

def foo():
    x = 1 + 2
    return x
```

### 2. Ruff Formatting

**What**: Code formatter (like Black, but faster)
**Auto-fixes**: No (check only - prevents commit if formatting needed)
**Fix manually**: `uv run ruff format .`

**What it checks**:
- Consistent indentation
- Line length (100 chars)
- Quote style
- Trailing commas

**Example**:
```python
# Bad formatting (fails check)
def foo(a,b,c):return a+b+c

# Good formatting (passes check)
def foo(a, b, c):
    return a + b + c
```

**To fix**:
```bash
# Format all files
uv run ruff format .

# Then commit
git add .
git commit -m "Fix formatting"
```

### 3. ty Type Checking

**What**: Static type checker for Python (Astral's Rust-based type checker)
**Auto-fixes**: No
**Config**: `pyproject.toml` → `[tool.ty]`

**What it checks**:
- Type annotations
- Type mismatches
- Missing return types
- Incorrect function calls

**Example**:
```python
# Type error (fails check)
def add(a: int, b: int) -> int:
    return str(a + b)  # Returns str, not int

# Fixed
def add(a: int, b: int) -> int:
    return a + b  # Returns int
```

**Common fixes**:
- Add type hints: `def foo(x: int) -> str:`
- Import types: `from typing import List, Dict, Optional`
- Fix return types to match annotations

### 4. Pytest Tests

**What**: Runs entire test suite
**Auto-fixes**: No
**Config**: `pyproject.toml`

**What it checks**:
- All tests pass
- No import errors
- No test failures

**Example**:
```bash
# Running tests manually
uv run pytest -xvs

# Fix failing tests before committing
```

---

## Configuration

### lefthook.yml

```yaml
pre-commit:
  parallel: false  # Run checks sequentially
  commands:
    ruff-check:
      glob: "*.py"
      run: uv run ruff check {staged_files}
      stage_fixed: true  # Auto-stage Ruff fixes

    ruff-format-check:
      glob: "*.py"
      run: uv run ruff format --check {staged_files}

    ty-check:
      glob: "*.py"
      run: uv run ty check {staged_files}

    pytest:
      run: uv run pytest -xvs --tb=short

fail_fast: true  # Stop on first failure
```

### Customize Checks

Edit `lefthook.yml` to:

**Skip specific checks**:
```yaml
pre-commit:
  commands:
    ty-check:
      skip: true  # Disable ty
```

**Change check order**:
```yaml
pre-commit:
  commands:
    pytest:  # Run tests first
      run: uv run pytest -xvs
    ruff-check:  # Then linting
      run: uv run ruff check {staged_files}
```

**Run in parallel** (faster but harder to read errors):
```yaml
pre-commit:
  parallel: true  # Run all checks simultaneously
```

---

## Troubleshooting

### Issue: Hooks Not Running

**Symptom**: Commits go through without checks

**Solutions**:
```bash
# 1. Check if hooks installed
ls -la .git/hooks/

# 2. Reinstall hooks
lefthook install

# 3. Verify lefthook.yml exists
cat lefthook.yml
```

### Issue: Ruff Check Fails

**Symptom**: `ruff check` fails with errors

**Solutions**:
```bash
# 1. See what's wrong
uv run ruff check .

# 2. Auto-fix issues
uv run ruff check --fix .

# 3. Commit fixes
git add .
git commit -m "Fix linting issues"
```

### Issue: Formatting Check Fails

**Symptom**: `ruff format --check` fails

**Solutions**:
```bash
# 1. Format all files
uv run ruff format .

# 2. Check what changed
git diff

# 3. Commit formatted code
git add .
git commit -m "Apply formatting"
```

### Issue: ty Fails

**Symptom**: Type checking errors

**Solutions**:
```bash
# 1. See errors
uv run ty check .

# 2. Add type hints
# Example: def foo(x: int) -> str:

# 3. Or add ty: ignore for specific lines
result = some_function()  # ty: ignore

# 4. Commit with fixes
git commit -m "Add type hints"
```

### Issue: Tests Fail

**Symptom**: pytest fails in pre-commit

**Solutions**:
```bash
# 1. Run tests manually to see details
uv run pytest -xvs

# 2. Fix failing tests

# 3. Verify all tests pass
uv run pytest

# 4. Commit
git commit -m "Fix tests"
```

### Issue: Hooks Too Slow

**Symptom**: Commits take too long

**Solutions**:

**1. Run checks in parallel** (edit `lefthook.yml`):
```yaml
pre-commit:
  parallel: true  # Faster but harder to debug
```

**2. Skip expensive checks**:
```bash
# Skip tests for this commit only
LEFTHOOK_EXCLUDE=pytest git commit -m "Quick fix"
```

**3. Only run on changed files**:
```yaml
# Edit lefthook.yml
ty-check:
  run: uv run ty check {staged_files}  # Only check changed files
```

---

## Best Practices

### Do

- **Install hooks immediately** after cloning the repo
- **Fix issues as you work**, don't wait for pre-commit to catch them
- **Run checks manually** during development:
  ```bash
  uv run ruff check .
  uv run ty check .
  uv run pytest
  ```
- **Commit often** with passing checks
- **Use descriptive commit messages**

### Don't

- **Don't skip hooks** unless absolutely necessary
- **Don't commit with failing tests** to main/master
- **Don't disable checks** without team agreement
- **Don't commit `--no-verify`** to production branches

---

## Manual Check Commands

Run checks manually without committing:

```bash
# Lint and auto-fix
uv run ruff check --fix .

# Format code
uv run ruff format .

# Type check
uv run ty check .

# Run tests
uv run pytest -xvs

# Run all checks (simulate pre-commit)
uv run ruff check . && \
uv run ruff format --check . && \
uv run ty check . && \
uv run pytest
```

---

## CI/CD Integration

Pre-commit hooks also run in CI/CD (GitHub Actions, GitLab CI, etc.):

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]

jobs:
  quality:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: |
          pip install uv
          uv sync

      - name: Run pre-commit checks
        run: |
          uv run ruff check .
          uv run ruff format --check .
          uv run ty check .
          uv run pytest
```

---

## Uninstall Hooks

To remove pre-commit hooks:

```bash
# Uninstall hooks
lefthook uninstall

# Verify
ls -la .git/hooks/  # pre-commit should be gone
```

To reinstall:
```bash
lefthook install
```

---

## Resources

- **Lefthook**: https://github.com/evilmartians/lefthook
- **Ruff**: https://docs.astral.sh/ruff/
- **ty**: https://docs.astral.sh/ty/
- **Pytest**: https://docs.pytest.org/

---

**Write better code with every commit!** 
