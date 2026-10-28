"""Advisory column-sensitivity classification over dataset schemas.

Everything here is a PROPOSAL for human review. Nothing is applied to the
platform: it does not touch lib/security.js policy state, does not write to
data/state.json, and does not gate any read path. A human reviewer must
confirm every proposal before any masking policy is configured.

The pure logic is stdlib-only and unit-testable with a stub router. The only
model interaction is `router.predict(state, questions)` on an injected router
(any object with that method, e.g. `laya.Router`), which is resolved lazily.
The model runs locally on cached weights; column names, types and any sample
values passed in are sent only to that local model, never to a network.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Tuple

__all__ = [
    "TIERS",
    "ClassifierError",
    "ColumnSensitivity",
    "ClassificationResult",
    "classify_columns",
]

# Ordered from most to least restrictive; the order is part of the contract.
TIERS: Tuple[str, str, str] = ("restricted", "internal", "public")
_TIER_INDEX = {tier: index for index, tier in enumerate(TIERS)}

# A noul answer at or above this probability may ADD an identifier flag to a
# column with no name-rule match. Measured yes-bias on the model's noul axis
# (schema-like inputs score ~0.75-0.85 even for counts and cities) keeps this
# deliberately high; the raw probability is always surfaced for review.
_IDENTIFIER_THRESHOLD = 0.9
_DEFAULT_CONFIDENCE = 0.5
_MAX_SAMPLES = 5
_MAX_SAMPLE_LENGTH = 40

_TIER_RULES: Tuple[Tuple[str, "re.Pattern[str]"], ...] = (
    (
        "restricted",
        re.compile(
            r"(^|_)(ssn|sin|tax_id|taxid|passport|national_id|gov_id|iban|swift"
            r"|card_number|card_no|credit_card|pan|salary|bank_account"
            r"|routing_number|dob|date_of_birth|health|medical|diagnosis"
            r"|biometric)(_|$)"
        ),
    ),
    (
        "internal",
        re.compile(
            r"(^|_)(email|phone|mobile|address|street|city|postal_code|zip"
            r"|postcode|first_name|last_name|full_name|customer_id|user_id"
            r"|account_id|employee_id)(_|$)"
        ),
    ),
)

_IDENTIFIER_RE = re.compile(r"(^|_)(id|uid|uuid|guid|key|code|number|no)(_|$)")


class ClassifierError(Exception):
    """Raised for invalid schemas, invalid router answers, or router failure."""


@dataclass(frozen=True)
class ColumnSensitivity:
    """One column's advisory proposal. `tier` is the proposed final tier."""

    column: str
    data_type: str
    tier: str
    probability: float
    confidence: float
    rule_tier: Optional[str]
    model_tier: Optional[str]
    identifier: bool
    identifier_probability: float
    basis: str


class ClassificationResult:
    """Proposals for one dataset schema, plus the router's routing metadata."""

    def __init__(
        self,
        dataset_name: Optional[str],
        proposals: Tuple[ColumnSensitivity, ...],
        routing: Mapping[str, Any],
    ) -> None:
        self.dataset_name = dataset_name
        self.proposals = proposals
        self.routing = dict(routing)

    def to_json(self, indent: Optional[int] = 2) -> str:
        payload = {
            "dataset": self.dataset_name,
            "routing": self.routing,
            "proposals": [
                {
                    "column": item.column,
                    "type": item.data_type,
                    "tier": item.tier,
                    "probability": item.probability,
                    "confidence": item.confidence,
                    "rule_tier": item.rule_tier,
                    "model_tier": item.model_tier,
                    "identifier": item.identifier,
                    "identifier_probability": item.identifier_probability,
                    "basis": item.basis,
                }
                for item in self.proposals
            ],
        }
        return json.dumps(payload, indent=indent)

    def __repr__(self) -> str:
        return (
            f"ClassificationResult(dataset_name={self.dataset_name!r}, "
            f"proposals={len(self.proposals)})"
        )


class _Router(Protocol):
    def predict(
        self, state: Any, questions: Mapping[str, Any]
    ) -> Mapping[str, Any]:  # pragma: no cover - protocol stub
        ...


def _rule_tier(column: str) -> Optional[str]:
    """Most restrictive tier whose name rule fires, or None."""
    for tier, pattern in _TIER_RULES:
        if pattern.search(column):
            return tier
    return None


def _schema_columns(
    schema: Any,
) -> Tuple[Optional[str], List[Mapping[str, str]]]:
    """Accept either a column list or a DatasetSchema-shaped mapping."""
    dataset_name: Optional[str] = None
    columns: Any = schema
    if isinstance(schema, Mapping):
        dataset_name = schema.get("name")
        columns = schema.get("columns", schema.get("properties"))
        if columns is None:
            raise ClassifierError(
                "schema mapping must contain a 'columns' list of column objects"
            )
    if not isinstance(columns, Iterable) or isinstance(columns, (str, bytes)):
        raise ClassifierError("schema must be an iterable of column objects")
    normalized: List[Mapping[str, str]] = []
    for entry in columns:
        if not isinstance(entry, Mapping):
            raise ClassifierError(f"column entry {entry!r} must be a mapping")
        name = entry.get("name")
        data_type = entry.get("type", entry.get("data_type"))
        if not isinstance(name, str) or not name.strip():
            raise ClassifierError(f"column entry {entry!r} needs a nonempty 'name'")
        if not isinstance(data_type, str) or not data_type.strip():
            raise ClassifierError(
                f"column '{name}' needs a nonempty 'type' (or 'data_type')"
            )
        normalized.append({"name": name, "type": data_type})
    if not normalized:
        raise ClassifierError("schema must declare at least one column")
    return dataset_name, normalized


def _format_samples(values: Iterable[Any]) -> str:
    chosen: List[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        chosen.append(text[:_MAX_SAMPLE_LENGTH])
        if len(chosen) == _MAX_SAMPLES:
            break
    return ", ".join(chosen)


def _tier_question(name: str, data_type: str, samples: str) -> Mapping[str, Any]:
    instructions = (
        f"Classify the sensitivity of dataset column '{name}' (type {data_type}) "
        "for access-control masking."
    )
    if samples:
        instructions += f" Sample values: {samples}."
    return {
        "type": "choice",
        "instructions": instructions,
        "criteria": {
            "restricted": "personal, financial, health, or credential data",
            "internal": "internal business data not intended for the public",
            "public": "safe to publish openly",
        },
    }


def _identifier_question(name: str, data_type: str, samples: str) -> Mapping[str, Any]:
    instructions = (
        f"Column '{name}' (type {data_type}) holds values like: {samples or 'unknown'}. "
        "Answer yes ONLY if its purpose is to uniquely identify each row "
        "(a primary or foreign key). Answer no for names, cities, counts, "
        "or measurements."
    )
    return {"type": "noul", "instructions": instructions}


def _choice_tier(answer: Any, column: str) -> Tuple[str, float, float]:
    """Decode a Laya choice answer into (tier, probability, confidence)."""
    if not isinstance(answer, Mapping):
        raise ClassifierError(f"column '{column}': answer is not an object")
    answer_type = answer.get("type")
    if answer_type is not None and answer_type != "choice":
        raise ClassifierError(
            f"column '{column}': expected a 'choice' answer, got {answer_type!r}"
        )
    choice = answer.get("choice")
    if not isinstance(choice, str) or choice not in _TIER_INDEX:
        raise ClassifierError(
            f"column '{column}': choice {choice!r} is not one of {list(TIERS)}"
        )
    probabilities = answer.get("probabilities")
    probability = _DEFAULT_CONFIDENCE
    if isinstance(probabilities, Mapping):
        raw = probabilities.get(choice)
        if isinstance(raw, (int, float)):
            probability = float(raw)
    confidence = answer.get("confidence", _DEFAULT_CONFIDENCE)
    if not isinstance(confidence, (int, float)):
        confidence = _DEFAULT_CONFIDENCE
    return choice, probability, float(confidence)


def _identifier_flag(answer: Any, column: str) -> Tuple[bool, float]:
    """Decode a Laya noul answer into (is_identifier, probability)."""
    if not isinstance(answer, Mapping):
        raise ClassifierError(f"column '{column}': answer is not an object")
    answer_type = answer.get("type")
    if answer_type is not None and answer_type != "noul":
        raise ClassifierError(
            f"column '{column}': expected a 'noul' answer, got {answer_type!r}"
        )
    probability = answer.get("noul")
    if not isinstance(probability, (int, float)):
        raise ClassifierError(f"column '{column}': noul answer has no numeric 'noul'")
    return float(probability) >= _IDENTIFIER_THRESHOLD, float(probability)


def classify_columns(
    schema: Any,
    samples: Optional[Mapping[str, Iterable[Any]]] = None,
    router: Optional[_Router] = None,
) -> ClassificationResult:
    """Propose a sensitivity tier and identifier flag for every column.

    `schema` is either an iterable of `{"name", "type"}` mappings or a
    DatasetSchema-shaped mapping with a `columns` list. `samples` optionally
    maps column name to a few example values; samples are truncated and only
    included in the local model's prompt. `router` defaults to a lazily
    constructed `laya.Router` (which must be installed, e.g. in `.venv`).
    """
    dataset_name, columns = _schema_columns(schema)
    sample_map = {
        name: _format_samples(values)
        for name, values in (samples or {}).items()
        if isinstance(name, str)
    }

    if router is None:
        try:
            from laya import Router  # imported lazily; laya is optional
        except ImportError as exc:  # pragma: no cover - depends on env
            raise ClassifierError(
                "laya is not installed; pass a router or install laya "
                "(python3 -m pip install laya)"
            ) from exc
        router = Router()

    questions: Dict[str, Mapping[str, Any]] = {}
    for column in columns:
        name = column["name"]
        question_samples = sample_map.get(name, "")
        questions[f"{name}/tier"] = _tier_question(
            name, column["type"], question_samples
        )
        questions[f"{name}/identifier"] = _identifier_question(
            name, column["type"], question_samples
        )

    state = {
        "task": "column sensitivity classification",
        "dataset": dataset_name,
        "columns": [
            {"name": column["name"], "type": column["type"]} for column in columns
        ],
    }
    try:
        result = router.predict(state, questions)
    except Exception as exc:  # adapter boundary: normalize third-party errors
        raise ClassifierError(f"router prediction failed: {exc}") from exc
    if not isinstance(result, Mapping):
        raise ClassifierError("router returned a non-object result")
    answers = result.get("answers")
    if not isinstance(answers, Mapping):
        raise ClassifierError("router result is missing an 'answers' object")
    routing = result.get("routing")
    if not isinstance(routing, Mapping):
        routing = {}

    proposals: List[ColumnSensitivity] = []
    for column in columns:
        name = column["name"]
        model_tier, probability, confidence = _choice_tier(
            answers.get(f"{name}/tier"), name
        )
        model_identifier, identifier_probability = _identifier_flag(
            answers.get(f"{name}/identifier"), name
        )
        # Rule-first: an identifier-shaped column name flags the column even
        # when the model disagrees; the model can only ADD flagless columns,
        # and only above the conservative threshold.
        identifier = bool(_IDENTIFIER_RE.search(name)) or model_identifier
        rule = _rule_tier(name)
        if rule is None:
            tier, basis = model_tier, "model"
        elif model_tier == rule:
            tier, basis = model_tier, "rule+model-agree"
        elif _TIER_INDEX[model_tier] < _TIER_INDEX[rule]:
            # A rule match is a floor: the model may escalate upward but never
            # downgrade below a name rule (advisory fail-closed default).
            tier, basis = model_tier, "model-escalates-rule"
        else:
            tier, basis = rule, "rule-overrides-model"
        proposals.append(
            ColumnSensitivity(
                column=name,
                data_type=column["type"],
                tier=tier,
                probability=probability,
                confidence=confidence,
                rule_tier=rule,
                model_tier=model_tier,
                identifier=identifier,
                identifier_probability=identifier_probability,
                basis=basis,
            )
        )

    return ClassificationResult(dataset_name, tuple(proposals), routing)
