from __future__ import annotations

from pathlib import Path

# Keep code under `codebase/` and write/read runtime artifacts from repo root.
ROOT_DIR = Path(__file__).resolve().parents[2]


def resolve_path(path: str | Path | None) -> Path | None:
    if path is None:
        return None
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return (ROOT_DIR / candidate).resolve()


def resolve_str(path: str | Path | None) -> str | None:
    resolved = resolve_path(path)
    if resolved is None:
        return None
    return str(resolved)
