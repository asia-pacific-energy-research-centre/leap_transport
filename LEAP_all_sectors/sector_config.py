"""Configuration objects describing how to process a LEAP sector."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Mapping, Optional, Sequence, Tuple

import pandas as pd

from .sector_core import BranchTuple, ExpressionMapping

DataFrame = pd.DataFrame

Preprocessor = Callable[[DataFrame], DataFrame]
Validator = Callable[[DataFrame], Tuple[DataFrame, Optional[pd.DataFrame]]]
MeasuresProcessor = Callable[
    [DataFrame, Mapping[str, Dict[str, object]], str, Sequence[str], BranchTuple, Tuple[str, ...]],
    Dict[str, DataFrame],
]
MetadataResolver = Callable[[Dict[str, object], BranchTuple, Tuple[str, ...]], Dict[str, object]]


@dataclass
class SectorConfig:
    """Container holding all configuration required to process a sector."""

    name: str
    leap_root: str
    leap_branch_to_source_map: Mapping[BranchTuple, Optional[Tuple[str, ...]]]
    shortname_to_leap_branches: Mapping[str, Sequence[BranchTuple]]
    measure_config: Mapping[str, Mapping[str, Dict[str, object]]]
    expression_mapping: ExpressionMapping
    measures_processor: MeasuresProcessor
    source_dimension_order: Sequence[str]
    preprocessors: Sequence[Preprocessor] = field(default_factory=list)
    validators: Sequence[Validator] = field(default_factory=list)
    share_normalisers: Sequence[Preprocessor] = field(default_factory=list)
    scenario_column: str = "Scenario"
    time_column: str = "Date"
    grouping_base_columns: Sequence[str] = field(default_factory=tuple)
    metadata_resolver: MetadataResolver = field(
        default_factory=lambda: (lambda meta, _leap, _src: meta)
    )
    log_extra_columns: Sequence[str] = field(default_factory=list)
    default_expression_mode: Tuple[str, Sequence[int]] = ("Data", ())

    def get_shortname_for_branch(self, branch: BranchTuple) -> Optional[str]:
        """Return the shortname that owns ``branch`` if known."""

        for shortname, branches in self.shortname_to_leap_branches.items():
            if branch in branches:
                return shortname
        return None


__all__ = [
    "SectorConfig",
    "Preprocessor",
    "Validator",
    "MeasuresProcessor",
    "MetadataResolver",
]
