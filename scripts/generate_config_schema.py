"""Regenerate config.schema.json and the README key table from src/config.py.

Run from the repository root: uv run python -m scripts.generate_config_schema
"""

from pathlib import Path

from src.config import Config
from src.config_schema import render_readme_table, render_schema_json, splice_readme

REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    readme_path = REPO_ROOT / "README.md"
    outputs = {
        REPO_ROOT / "config.schema.json": render_schema_json(Config),
        readme_path: splice_readme(
            readme_path.read_text(encoding="utf-8"), render_readme_table(Config)
        ),
    }

    for path, text in outputs.items():
        if path.exists() and path.read_text(encoding="utf-8") == text:
            continue

        path.write_text(text, encoding="utf-8", newline="\n")

        print(f"Updated {path.relative_to(REPO_ROOT).as_posix()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
