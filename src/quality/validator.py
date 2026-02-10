"""
Core validation engine.

DataValidator orchestrates rule evaluation, collects results,
and produces a ValidationReport.
"""

from typing import List, Optional

import pandas as pd

from .rules import Rule, RuleSet, RuleResult
from .report import ValidationReport


class DataValidator:
    """
    Validate a DataFrame against a set of rules.

    Usage:
        from quality import DataValidator, CompletenessRule, RangeRule

        v = DataValidator("sec_filings")
        v.add_rule(CompletenessRule(["cik", "company_name"]))
        v.add_rule(RangeRule("revenue", min_val=0))

        report = v.validate(df)
        report.print_summary()

        if not report.passed:
            report.print_failures()
    """

    def __init__(self, name: str = 'validation'):
        self.name = name
        self._ruleset = RuleSet(name)

    def add_rule(self, rule: Rule) -> 'DataValidator':
        """Add a single rule."""
        self._ruleset.add(rule)
        return self

    def add_rules(self, rules: List[Rule]) -> 'DataValidator':
        """Add multiple rules at once."""
        for rule in rules:
            self._ruleset.add(rule)
        return self

    @property
    def rule_count(self) -> int:
        return len(self._ruleset)

    def validate(self, df: pd.DataFrame) -> 'ValidationReport':
        """
        Run all rules against the DataFrame.

        Returns a ValidationReport with pass/fail status,
        individual results, and summary statistics.
        """
        results = self._ruleset.evaluate(df)

        return ValidationReport(
            name=self.name,
            results=results,
            row_count=len(df),
            column_count=len(df.columns),
        )
