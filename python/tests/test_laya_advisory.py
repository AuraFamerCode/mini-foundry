from __future__ import annotations

import json
import unittest
from typing import Any, Dict, Iterable, List, Mapping, Optional

from python.laya_advisory.classifier import (
    TIERS,
    ClassifierError,
    classify_columns,
)


class StubRouter:
    """Deterministic laya.Router stand-in recording predict() calls."""

    def __init__(
        self,
        tiers: Optional[Dict[str, str]] = None,
        identifier_flags: Optional[Dict[str, float]] = None,
        confidences: Optional[Dict[str, float]] = None,
        fail: bool = False,
    ) -> None:
        self.tiers = tiers or {}
        self.identifier_flags = identifier_flags or {}
        self.confidences = confidences or {}
        self.fail = fail
        self.calls: List[Mapping[str, Any]] = []

    def predict(self, state: Any, questions: Mapping[str, Any]) -> Mapping[str, Any]:
        if self.fail:
            raise RuntimeError("router offline")
        self.calls.append({"state": state, "questions": dict(questions)})
        answers: Dict[str, Any] = {}
        for key, question in questions.items():
            if question["type"] == "choice":
                choice = self.tiers.get(key, "public")
                answers[key] = {
                    "type": "choice",
                    "choice": choice,
                    "probabilities": {option: 0.1 for option in question["criteria"]},
                    "confidence": self.confidences.get(key, 0.9),
                }
                answers[key]["probabilities"][choice] = 0.8
            else:
                probability = self.identifier_flags.get(key, 0.1)
                answers[key] = {
                    "type": "noul",
                    "noul": probability,
                    "confidence": probability,
                }
        return {"answers": answers, "routing": {"model": "stub"}}


SCHEMA = [
    {"name": "employee_id", "type": "String"},
    {"name": "full_name", "type": "String"},
    {"name": "salary", "type": "Number"},
    {"name": "office_city", "type": "String"},
]


class ColumnRuleTests(unittest.TestCase):
    def test_restricted_rule_fires_without_model_help(self) -> None:
        router = StubRouter(tiers={"salary/tier": "public"})
        result = classify_columns(SCHEMA, router=router)
        salary = next(p for p in result.proposals if p.column == "salary")
        self.assertEqual(salary.tier, "restricted")
        self.assertEqual(salary.rule_tier, "restricted")
        self.assertEqual(salary.model_tier, "public")
        self.assertEqual(salary.basis, "rule-overrides-model")
        self.assertFalse(salary.identifier)  # no identifier token in the name

    def test_rule_floor_overrides_model_and_agreement_is_stable(self) -> None:
        router = StubRouter(
            tiers={"full_name/tier": "public", "office_city/tier": "internal"}
        )
        result = classify_columns(SCHEMA, router=router)
        by_name = {p.column: p for p in result.proposals}
        self.assertEqual(by_name["full_name"].tier, "internal")
        self.assertEqual(by_name["full_name"].basis, "rule-overrides-model")
        self.assertEqual(by_name["office_city"].tier, "internal")
        self.assertEqual(by_name["office_city"].basis, "rule+model-agree")

    def test_confident_model_cannot_escalate_above_rule_but_may_downgrade(self) -> None:
        # Model says restricted for office_city: accepted (upward move).
        router = StubRouter(tiers={"office_city/tier": "restricted"})
        result = classify_columns(SCHEMA, router=router)
        city = next(p for p in result.proposals if p.column == "office_city")
        self.assertEqual(city.tier, "restricted")
        self.assertEqual(city.basis, "model-escalates-rule")
        # A rule match is a floor: even a confident downgrade does not pass.
        router_low = StubRouter(tiers={"full_name/tier": "public"})
        result_low = classify_columns(SCHEMA, router=router_low)
        name = next(p for p in result_low.proposals if p.column == "full_name")
        self.assertEqual(name.tier, "internal")
        self.assertEqual(name.basis, "rule-overrides-model")

    def test_identifier_flags_are_rule_first_with_conservative_model_add(self) -> None:
        # Rule-shaped column names flag even when the model disagrees.
        router = StubRouter(identifier_flags={"employee_id/identifier": 0.1})
        result = classify_columns(SCHEMA, router=router)
        employee = next(p for p in result.proposals if p.column == "employee_id")
        self.assertTrue(employee.identifier)  # name rule, model said no
        # A rule-less column needs the model at the conservative threshold.
        at_threshold = StubRouter(identifier_flags={"office_city/identifier": 0.9})
        result_at = classify_columns(SCHEMA, router=at_threshold)
        city = next(p for p in result_at.proposals if p.column == "office_city")
        self.assertTrue(city.identifier)
        self.assertGreaterEqual(city.identifier_probability, 0.9)
        below_threshold = StubRouter(
            identifier_flags={"office_city/identifier": 0.89}
        )
        result_below = classify_columns(SCHEMA, router=below_threshold)
        city_below = next(
            p for p in result_below.proposals if p.column == "office_city"
        )
        self.assertFalse(city_below.identifier)
        self.assertAlmostEqual(city_below.identifier_probability, 0.89)


class ResultShapeTests(unittest.TestCase):
    def test_questions_are_bounded_and_state_carries_schema_only(self) -> None:
        router = StubRouter()
        samples = {"full_name": ["Ada Lovelace", "Lin T", None, ""]}
        classify_columns(SCHEMA, samples=samples, router=router)
        self.assertEqual(len(router.calls), 1)
        questions = router.calls[0]["questions"]
        self.assertEqual(len(questions), 2 * len(SCHEMA))
        name_tier = questions["full_name/tier"]
        self.assertEqual(name_tier["type"], "choice")
        self.assertEqual(list(name_tier["criteria"]), list(TIERS))
        self.assertIn("Ada Lovelace", name_tier["instructions"])
        self.assertNotIn("None", name_tier["instructions"])
        state = router.calls[0]["state"]
        self.assertEqual(state["columns"], SCHEMA)
        self.assertNotIn("samples", json.dumps(state))

    def test_routing_metadata_and_json_shape_survive(self) -> None:
        result = classify_columns(SCHEMA, router=StubRouter())
        self.assertEqual(result.routing, {"model": "stub"})
        payload = json.loads(result.to_json())
        self.assertEqual(payload["routing"], {"model": "stub"})
        self.assertEqual(len(payload["proposals"]), len(SCHEMA))
        for proposal in payload["proposals"]:
            self.assertIn(proposal["tier"], TIERS)
            self.assertIsInstance(proposal["identifier"], bool)
            self.assertIn(
                proposal["basis"],
                {
                    "model",
                    "rule+model-agree",
                    "model-escalates-rule",
                    "rule-overrides-model",
                },
            )

    def test_dataset_schema_mapping_is_accepted(self) -> None:
        router = StubRouter()
        result = classify_columns(
            {"name": "employees", "columns": SCHEMA}, router=router
        )
        self.assertEqual(result.dataset_name, "employees")
        self.assertEqual(len(result.proposals), 4)


class ValidationTests(unittest.TestCase):
    def test_invalid_inputs_fail_closed(self) -> None:
        with self.assertRaises(ClassifierError):
            classify_columns([], router=StubRouter())
        with self.assertRaises(ClassifierError):
            classify_columns([{"name": "", "type": "String"}], router=StubRouter())
        with self.assertRaises(ClassifierError):
            classify_columns([{"name": "a"}], router=StubRouter())
        # A single valid column classifies fine (no error expected).
        result = classify_columns([{"name": "a", "type": "String"}], router=StubRouter())
        self.assertEqual(result.proposals[0].model_tier, "public")
        with self.assertRaises(ClassifierError):
            classify_columns({"nope": 1}, router=StubRouter())

    def test_router_failures_are_normalized(self) -> None:
        with self.assertRaises(ClassifierError):
            classify_columns(SCHEMA, router=StubRouter(fail=True))

    def test_wrong_answer_shapes_are_rejected(self) -> None:
        class BadShape:
            def predict(self, state: Any, questions: Mapping[str, Any]) -> Any:
                return {
                    "answers": {
                        list(questions)[0]: {"type": "score", "score": 1.0}
                    },
                    "routing": {},
                }

        with self.assertRaises(ClassifierError):
            classify_columns(SCHEMA[:1], router=BadShape())

    def test_missing_router_without_laya_reports_clear_error(self) -> None:
        import builtins
        import sys
        import unittest.mock

        classifier = sys.modules["python.laya_advisory.classifier"]
        with unittest.mock.patch.dict(sys.modules, {"laya": None}):
            with self.assertRaises(ClassifierError) as caught:
                classifier.classify_columns(SCHEMA)
            self.assertIn("laya is not installed", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
