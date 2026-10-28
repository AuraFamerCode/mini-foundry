#!/usr/bin/env python3
"""Live demo: propose column-masking policies for a sample schema with Laya.

Run with the project venv (has laya installed and cached weights):
    .venv/bin/python demo_laya_advisory.py

Illustrates the intended use: one local model call proposes a sensitivity
tier and an identifier flag per column; output is advisory JSON for human
review, mirroring the masking-policy concepts enforced by lib/security.js.
"""

from python.laya_advisory import classify_columns

SCHEMA = {
    "name": "employees",
    "columns": [
        {"name": "employee_id", "type": "String"},
        {"name": "full_name", "type": "String"},
        {"name": "work_email", "type": "String"},
        {"name": "salary", "type": "Number"},
        {"name": "bank_account", "type": "String"},
        {"name": "date_of_birth", "type": "Date"},
        {"name": "office_city", "type": "String"},
        {"name": "headcount", "type": "Integer"},
    ],
}

SAMPLES = {
    "employee_id": ["E-1001", "E-1002"],
    "full_name": ["Ada Lovelace", "Lin Zhao"],
    "work_email": ["ada@acme.test", "lin@acme.test"],
    "salary": ["120000", "98000"],
    "bank_account": ["DE89 3704 0044 0532 0130 00"],
    "date_of_birth": ["1985-04-12", "1990-11-30"],
    "office_city": ["Berlin", "Lisbon"],
    "headcount": ["42"],
}


def main() -> None:
    result = classify_columns(SCHEMA, samples=SAMPLES)
    print(f"routing: {result.routing.get('model')} ({result.routing.get('reason')})")
    print(result.to_json())
    print("\nAdvisory only. A human must review these proposals before any")
    print("masking policy is configured (lib/security.js enforcement model).")


if __name__ == "__main__":
    main()
