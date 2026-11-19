"""Validation for config/tables.json — the framework's metadata contract.

Files 1-3 of this project consume tables.json with an implicit contract:
some missing fields crash (cfg["..."]), others silently default (.get),
and the loader treats any load_type other than exactly "full" as
incremental — so a typo like "incremental " or "delta" silently picks
the wrong load strategy, and a broken entry still generates a full DAG
task chain that only fails at run time.

validate_tables() makes the contract explicit and enforced in ONE place.
It is called:
  - by the DAG at parse time (a bad entry fails the parse loudly instead
    of generating a misleading task chain), and
  - by the loader's load_config() (standalone runs are protected too).

Pure standard library — safe to import at DAG parse time.
"""
import re

NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
LOAD_TYPES = ("incremental", "full")
REQUIRED_FIELDS = ("name", "source_path", "target_path", "load_type",
                   "primary_keys")


class ConfigError(ValueError):
    """Raised when a tables.json entry breaks the metadata contract."""


def validate_tables(tables) -> list:
    """Validate the parsed 'tables' list; return it unchanged if valid.

    Raises ConfigError with a message naming the table and the problem.
    """
    if not isinstance(tables, list) or not tables:
        raise ConfigError("config must contain a non-empty 'tables' list")

    seen_names = set()
    for entry in tables:
        if not isinstance(entry, dict):
            raise ConfigError(f"table entry must be an object, got {entry!r}")
        name = entry.get("name", "<unnamed>")

        for field in REQUIRED_FIELDS:
            if field not in entry:
                raise ConfigError(f"{name}: missing required field '{field}'")

        if not isinstance(name, str) or not NAME_PATTERN.match(name):
            raise ConfigError(
                f"{name!r}: name must be letters/digits/underscore only "
                "(it becomes Airflow task ids and a shell argument)")
        if name in seen_names:
            raise ConfigError(f"{name}: duplicate table name")
        seen_names.add(name)

        load_type = entry["load_type"]
        if load_type not in LOAD_TYPES:
            raise ConfigError(
                f"{name}: load_type must be one of {LOAD_TYPES}, "
                f"got {load_type!r} (exact match — a typo must not "
                "silently select a load strategy)")

        if load_type == "incremental" and not entry.get("watermark_column"):
            raise ConfigError(
                f"{name}: incremental load requires a watermark_column")

        keys = entry["primary_keys"]
        if not isinstance(keys, list) or not keys:
            raise ConfigError(
                f"{name}: primary_keys must be a non-empty list")

        min_rows = entry.get("min_rows", 1)
        if not isinstance(min_rows, int) or min_rows < 0:
            raise ConfigError(
                f"{name}: min_rows must be a non-negative integer, "
                f"got {min_rows!r}")

        not_null = entry.get("not_null_columns", [])
        if not isinstance(not_null, list):
            raise ConfigError(
                f"{name}: not_null_columns must be a list, got {not_null!r}")

    return tables
