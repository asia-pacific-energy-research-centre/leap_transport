from __future__ import annotations

from dataclasses import dataclass


TRANSPORT_ROOT = "Demand"
TRANSPORT_NON_ROAD_CONTAINER = "Transport non road"

ROAD_TOP_LEVEL_BRANCHES = frozenset(
    {
        "Passenger road",
        "Freight road",
    }
)

NON_ROAD_TOP_LEVEL_BRANCHES = frozenset(
    {
        "Passenger non road",
        "Freight non road",
        "International transport",
        "Pipeline transport",
        "Nonspecified transport",
    }
)

TRANSPORT_TOP_LEVEL_BRANCHES = ROAD_TOP_LEVEL_BRANCHES | NON_ROAD_TOP_LEVEL_BRANCHES


@dataclass(frozen=True)
class ParsedTransportBranchPath:
    root: str | None
    logical_tuple: tuple[str, ...]
    has_non_road_container: bool


def _clean_parts(parts: list[str]) -> list[str]:
    return [part.strip() for part in parts if part and str(part).strip()]


def is_transport_branch_tuple(branch_tuple: tuple[str, ...] | list[str]) -> bool:
    if not branch_tuple:
        return False
    return str(branch_tuple[0]).strip() in TRANSPORT_TOP_LEVEL_BRANCHES


def is_non_road_branch_tuple(branch_tuple: tuple[str, ...] | list[str]) -> bool:
    if not branch_tuple:
        return False
    return str(branch_tuple[0]).strip() in NON_ROAD_TOP_LEVEL_BRANCHES


def build_transport_branch_path(
    branch_tuple: tuple[str, ...] | list[str],
    *,
    root: str = TRANSPORT_ROOT,
) -> str:
    logical_tuple = tuple(str(part).strip() for part in branch_tuple if str(part).strip())
    if not logical_tuple:
        return str(root).strip()

    root_parts = _clean_parts(str(root).split("\\"))
    parts = root_parts or [TRANSPORT_ROOT]
    if (
        is_non_road_branch_tuple(logical_tuple)
        and (not parts or parts[-1] != TRANSPORT_NON_ROAD_CONTAINER)
    ):
        parts.append(TRANSPORT_NON_ROAD_CONTAINER)
    parts.extend(logical_tuple)
    return "\\".join(parts)


def parse_transport_branch_path(branch_path: str) -> ParsedTransportBranchPath:
    parts = _clean_parts(str(branch_path or "").split("\\"))
    if not parts:
        return ParsedTransportBranchPath(root=None, logical_tuple=(), has_non_road_container=False)

    root: str | None = None
    logical_parts = parts
    has_non_road_container = False

    if parts[0] == TRANSPORT_ROOT:
        root = parts[0]
        logical_parts = parts[1:]

    if logical_parts and logical_parts[0] == TRANSPORT_NON_ROAD_CONTAINER:
        has_non_road_container = True
        logical_parts = logical_parts[1:]

    if logical_parts and logical_parts[0] not in TRANSPORT_TOP_LEVEL_BRANCHES:
        return ParsedTransportBranchPath(
            root=root,
            logical_tuple=tuple(logical_parts),
            has_non_road_container=has_non_road_container,
        )

    return ParsedTransportBranchPath(
        root=root,
        logical_tuple=tuple(logical_parts),
        has_non_road_container=has_non_road_container,
    )


def extract_transport_branch_tuple(branch_path: str) -> tuple[str, ...]:
    return parse_transport_branch_path(branch_path).logical_tuple


def is_non_road_transport_branch_path(branch_path: str) -> bool:
    logical_tuple = extract_transport_branch_tuple(branch_path)
    return is_non_road_branch_tuple(logical_tuple)


def is_pipeline_or_nonspecified_branch_path(branch_path: str) -> bool:
    logical_tuple = extract_transport_branch_tuple(branch_path)
    if not logical_tuple:
        return False
    return logical_tuple[0] in {"Pipeline transport", "Nonspecified transport"}


def branch_tuple_depth(branch_path: str) -> int:
    return len(extract_transport_branch_tuple(branch_path))


def transport_branch_path_candidates(branch_path: str) -> list[str]:
    parsed = parse_transport_branch_path(branch_path)
    if not parsed.logical_tuple:
        cleaned = "\\".join(_clean_parts(str(branch_path or "").split("\\")))
        return [cleaned] if cleaned else []

    root = parsed.root or TRANSPORT_ROOT
    canonical = build_transport_branch_path(parsed.logical_tuple, root=root)
    candidates = [canonical]

    if is_non_road_branch_tuple(parsed.logical_tuple):
        legacy_parts = _clean_parts(str(root).split("\\"))
        if legacy_parts and legacy_parts[-1] == TRANSPORT_NON_ROAD_CONTAINER:
            legacy_parts = legacy_parts[:-1]
        legacy_root = "\\".join(legacy_parts) if legacy_parts else TRANSPORT_ROOT
        legacy = "\\".join([legacy_root, *parsed.logical_tuple])
        if legacy not in candidates:
            candidates.append(legacy)

    return candidates
