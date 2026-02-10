"""
Validation rule definitions.

Each rule encapsulates a single data quality check. Rules are composable
via RuleSet and return structured results for reporting.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import pandas as pd


@dataclass
class RuleResult:
    """Result of a single rule evaluation."""
    rule_name: str
    passed: bool
    column: Optional[str]
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def severity(self) -> str:
        return 'PASS' if self.passed else 'FAIL'


class Rule(ABC):
    """Base class for all validation rules."""

    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__

    @abstractmethod
    def evaluate(self, df: pd.DataFrame) -> RuleResult:
        """Run this rule against a DataFrame and return a result."""
        ...


class CompletenessRule(Rule):
    """
    Check that required columns have no null values.

    Args:
        columns: Column names to check.
        threshold: Minimum completeness ratio (0.0 to 1.0). Default 1.0 (no nulls).
    """

    def __init__(self, columns: List[str], threshold: float = 1.0, name: Optional[str] = None):
        super().__init__(name or f"completeness_{','.join(columns)}")
        self.columns = columns
        self.threshold = threshold

    def evaluate(self, df: pd.DataFrame) -> RuleResult:
        failures = {}
        for col in self.columns:
            if col not in df.columns:
                failures[col] = {'error': 'column not found'}
                continue
            total = len(df)
            non_null = int(df[col].notna().sum())
            ratio = non_null / total if total > 0 else 1.0
            if ratio < self.threshold:
                failures[col] = {
                    'completeness': round(ratio, 4),
                    'null_count': total - non_null,
                    'threshold': self.threshold,
                }

        return RuleResult(
            rule_name=self.name,
            passed=len(failures) == 0,
            column=','.join(self.columns),
            details={'failures': failures} if failures else {},
        )


class UniquenessRule(Rule):
    """
    Check that key columns contain no duplicate rows.

    Args:
        columns: Columns that form the unique key.
    """

    def __init__(self, columns: List[str], name: Optional[str] = None):
        super().__init__(name or f"uniqueness_{','.join(columns)}")
        self.columns = columns

    def evaluate(self, df: pd.DataFrame) -> RuleResult:
        missing = [c for c in self.columns if c not in df.columns]
        if missing:
            return RuleResult(
                rule_name=self.name,
                passed=False,
                column=','.join(self.columns),
                details={'error': f'missing columns: {missing}'},
            )

        dup_mask = df.duplicated(subset=self.columns, keep=False)
        dup_count = int(dup_mask.sum())
        return RuleResult(
            rule_name=self.name,
            passed=dup_count == 0,
            column=','.join(self.columns),
            details={
                'duplicate_rows': dup_count,
                'unique_rows': len(df) - dup_count,
                'total_rows': len(df),
            },
        )


class RangeRule(Rule):
    """
    Check that numeric values fall within an expected range.

    Args:
        column: Column to validate.
        min_val: Minimum allowed value (inclusive). None to skip.
        max_val: Maximum allowed value (inclusive). None to skip.
    """

    def __init__(
        self,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        name: Optional[str] = None,
    ):
        super().__init__(name or f"range_{column}")
        self.column = column
        self.min_val = min_val
        self.max_val = max_val

    def evaluate(self, df: pd.DataFrame) -> RuleResult:
        if self.column not in df.columns:
            return RuleResult(
                rule_name=self.name,
                passed=False,
                column=self.column,
                details={'error': f'column {self.column!r} not found'},
            )

        values = df[self.column].dropna()
        violations = 0

        if self.min_val is not None:
            violations += int((values < self.min_val).sum())
        if self.max_val is not None:
            violations += int((values > self.max_val).sum())

        return RuleResult(
            rule_name=self.name,
            passed=violations == 0,
            column=self.column,
            details={
                'violations': violations,
                'checked': len(values),
                'min_found': float(values.min()) if len(values) > 0 else None,
                'max_found': float(values.max()) if len(values) > 0 else None,
                'min_allowed': self.min_val,
                'max_allowed': self.max_val,
            },
        )


class PatternRule(Rule):
    """
    Check that string values match a regex pattern.

    Args:
        column: Column to validate.
        pattern: Regular expression (matched with str.match).
    """

    def __init__(self, column: str, pattern: str, name: Optional[str] = None):
        super().__init__(name or f"pattern_{column}")
        self.column = column
        self.pattern = pattern

    def evaluate(self, df: pd.DataFrame) -> RuleResult:
        if self.column not in df.columns:
            return RuleResult(
                rule_name=self.name,
                passed=False,
                column=self.column,
                details={'error': f'column {self.column!r} not found'},
            )

        values = df[self.column].dropna().astype(str)
        matches = values.str.match(self.pattern)
        mismatches = int((~matches).sum())

        return RuleResult(
            rule_name=self.name,
            passed=mismatches == 0,
            column=self.column,
            details={
                'mismatches': mismatches,
                'checked': len(values),
                'pattern': self.pattern,
            },
        )


class CustomRule(Rule):
    """
    User-defined validation via a callable.

    Args:
        func: A function that takes a DataFrame and returns (bool, dict).
              The bool is pass/fail, the dict is additional details.
        column: Optional column name for reporting.
    """

    def __init__(
        self,
        func: Callable[[pd.DataFrame], tuple],
        name: str = 'custom_rule',
        column: Optional[str] = None,
    ):
        super().__init__(name)
        self.func = func
        self.column = column

    def evaluate(self, df: pd.DataFrame) -> RuleResult:
        passed, details = self.func(df)
        return RuleResult(
            rule_name=self.name,
            passed=bool(passed),
            column=self.column,
            details=details,
        )


class RuleSet:
    """
    A named collection of rules that run together.

    Usage:
        rules = RuleSet("my_checks")
        rules.add(CompletenessRule(["id", "name"]))
        rules.add(RangeRule("amount", min_val=0))
        results = rules.evaluate(df)
    """

    def __init__(self, name: str = 'default'):
        self.name = name
        self.rules: List[Rule] = []

    def add(self, rule: Rule) -> 'RuleSet':
        self.rules.append(rule)
        return self

    def evaluate(self, df: pd.DataFrame) -> List[RuleResult]:
        return [rule.evaluate(df) for rule in self.rules]

    def __len__(self) -> int:
        return len(self.rules)
