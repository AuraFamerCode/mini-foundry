"""Advisory column-sensitivity classification for dataset schemas (optional).

Uses the Laya typed-decision model to PROPOSE column policies: a sensitivity
tier per column plus a guess whether the column holds a row identifier. Every
answer is a bounded decision with probabilities; nothing here is applied
automatically. A human must review proposals before any masking policy is
configured in the platform (see lib/security.js for the enforcement model).

This package is optional and separate from `foundry_engine`, which remains
stdlib-only. Import of `laya` happens lazily; inject a stub router for tests.
The model runs locally on cached weights; column names, types and any samples
you pass are sent only to that local model, never to a network service.
"""

from .classifier import (
    ClassifierError,
    ClassificationResult,
    ColumnSensitivity,
    TIERS,
    classify_columns,
)

__all__ = [
    "ClassifierError",
    "ClassificationResult",
    "ColumnSensitivity",
    "TIERS",
    "classify_columns",
]
