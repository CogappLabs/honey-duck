---
title: Debugging
description: Debug Dagster asset code with breakpoints using debugpy and VS Code, including variable inspection with Data Wrangler.
---

# Debugging Guide

Debug Dagster asset code with full breakpoint support, variable inspection, and Data Wrangler integration.

## Quick Start

```bash
# Start Dagster with debug mode
DAGSTER_DEBUG=1 uv run dg dev
```

Then in VS Code:

1. Select **"Dagster: Attach to dg dev"** from the debug dropdown
2. Press **F5** to attach
3. Set breakpoints in any asset file
4. Materialize from the Dagster UI at `http://localhost:3000`

## Setup

Two pieces make this work: the debugpy hook in `definitions.py` and the VS Code launch configuration.

### 1. The debugpy Hook

The debug hook lives at the top of `definitions.py`, before any asset imports. It runs when Dagster loads the code location and again in each step subprocess:

```python
--8<-- "src/honey_duck/defs/definitions.py:48:63"
```

This handles two scenarios:

| Process | What happens |
|---------|-------------|
| **Code server** (parent) | `debugpy.listen()` succeeds, binds to port 5678. VS Code attaches here. |
| **Step subprocess** | `debugpy.listen()` raises `RuntimeError` (port taken). We call `trace_this_thread(True)` and sleep 0.5s so the debug adapter can propagate breakpoints from VS Code before asset code runs. |

!!! info "Why the sleep?"
    Dagster's multiprocess executor spawns a new subprocess for each step. debugpy auto-attaches to these subprocesses, but VS Code needs time to send breakpoint locations. Without the 0.5s pause, the subprocess starts executing asset code before breakpoints are set — a race condition that causes breakpoints to be silently skipped.

### 2. The Launch Configuration

The attach configuration in `.vscode/launch.json` connects VS Code to the debugpy listener:

```json
--8<-- ".vscode/launch.json:68:73"
```

### 3. The debugpy Dependency

debugpy is a dev dependency in `pyproject.toml`:

```toml
[dependency-groups]
dev = [
    "debugpy>=1.8.20",
    ...
]
```

## Debugging Workflow

### With the Dagster UI

This is the primary workflow — use the UI to select and materialize specific assets while breakpoints are active:

```bash
DAGSTER_DEBUG=1 uv run dg dev
```

1. Wait for `debugpy listening on port 5678` in the terminal
2. **F5** with "Dagster: Attach to dg dev"
3. Set breakpoints in asset files (e.g. `src/honey_duck/defs/polars/assets.py`)
4. Open `http://localhost:3000`, navigate to an asset, click **Materialize**
5. VS Code pauses at the breakpoint with full variable inspection

### Without the UI

For quick iteration without a browser, the launch configurations run `dagster job execute` directly in a single process:

```json
--8<-- ".vscode/launch.json:5:12"
```

Select the configuration from the debug dropdown and press **F5**. The job executes with debugpy attached — no subprocess complications.

!!! tip "When to use each approach"
    Use **attach to dg dev** when you want to pick specific assets to materialize from the UI. Use the **direct launch** configs when you want to run a full pipeline and don't need the UI.

## Variable Inspection

While paused at a breakpoint, the Variables panel shows all local variables including Polars DataFrames and LazyFrames.

### Data Wrangler

Right-click a DataFrame variable and select **Open in Data Wrangler** for a tabular view with filtering, sorting, and summary statistics.

!!! note "Requirements"
    Data Wrangler requires the [ms-toolsai.datawrangler](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.datawrangler) and [ms-toolsai.jupyter](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter) VS Code extensions, plus `ipykernel` in your Python environment.

### Slow Attribute Warning

You may see this warning when inspecting Polars DataFrames:

```
pydevd warning: Getting attribute DataFrame.plot was slow (took 0.61s)
```

This is harmless — debugpy is resolving the `.plot` attribute which triggers Altair lazy loading. Suppress it by setting `PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT=2` in your environment or launch config.

## Debugging in Containers

!!! warning "Limited support"
    The `DAGSTER_DEBUG=1 uv run dg dev` + attach workflow does **not** work reliably in devcontainers. The debugpy subprocess auto-attach mechanism fails with "Client not authenticated" errors when VS Code is running via the Remote Containers extension.

    For debugging inside containers, use the direct launch configurations (`dagster job execute`) which run everything in a single process.

## Technical Details

### Architecture

```
                  VS Code (debugpy client)
                         │
                    port 5678
                         │
    dg dev ──► code server (debugpy.listen)
                    │
                    ├── run process
                    │     ├── step subprocess (auto-attach, 0.5s sync)
                    │     ├── step subprocess (auto-attach, 0.5s sync)
                    │     └── ...
                    │
                    └── webserver (port 3000)
```

### Frozen Modules (Python 3.12+)

Python 3.12+ uses frozen modules for stdlib imports. debugpy warns this may cause missed breakpoints:

```
Debugger warning: It seems that frozen modules are being used,
which may make the debugger miss breakpoints.
```

We suppress this with `PYDEVD_DISABLE_FILE_VALIDATION=1`. User code breakpoints are not affected — the warning only applies to stdlib frozen modules.

### In-Process Executor Does Not Help

Setting `executor=dg.in_process_executor` on `Definitions` prevents step-level subprocesses, but Dagster's `DefaultRunLauncher` still spawns a separate process for each **run**. The executor controls how steps within a run execute, not how runs are launched. The debugpy listener runs in the code server process, not the run process — so the in-process executor alone doesn't solve the debugging problem.

### Port Conflicts

The debugpy listener binds to `localhost:5678`. If another process is using this port, `dg dev` starts normally but debugging is silently unavailable — the `RuntimeError` is caught. Check for port conflicts if the "debugpy listening" message doesn't appear on startup.

### Programmatic Breakpoints

If UI breakpoints aren't hitting (e.g. the 0.5s sleep isn't enough on a slow machine), you can add a programmatic breakpoint directly in asset code:

```python
import debugpy; debugpy.breakpoint()
```

This always works regardless of propagation timing. Remove it when you're done debugging.

## References

- [debugpy documentation](https://github.com/microsoft/debugpy)
- [VS Code Python debugging](https://code.visualstudio.com/docs/python/debugging)
- [Data Wrangler extension](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.datawrangler)
