"""Dagster definitions entry point for honey-duck.

This module uses load_from_defs_folder() to auto-discover all definitions
in the defs/ directory. Each module decorated with @dg.definitions is
automatically merged into the final Definitions object.
"""

from pathlib import Path

import dagster as dg


@dg.definitions
def defs() -> dg.Definitions:
    """Load all Dagster definitions from the defs/ folder."""
    return dg.load_from_defs_folder(project_root=Path(__file__).parent.parent)
