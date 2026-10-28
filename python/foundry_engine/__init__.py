"""Polyglot data platform services. Core transform execution uses stdlib only."""

from .incremental import (
    DatasetStore,
    IncrementalError,
    IncrementalRunner,
    InputSlice,
    OutputDelta,
    TransformSpec,
    filter_map_transform,
)

__all__ = [
    "DatasetStore",
    "IncrementalError",
    "IncrementalRunner",
    "InputSlice",
    "OutputDelta",
    "TransformSpec",
    "filter_map_transform",
]
