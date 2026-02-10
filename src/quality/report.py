"""
Validation report generation.

Structures rule results into a human-readable report with
pass/fail counts, failure details, and summary statistics.
"""

from dataclasses import dataclass
from typing import Dict, List

from .rules import RuleResult


@dataclass
class ValidationReport:
    """
    Structured output from a validation run.

    Attributes:
        name: Name of this validation run.
        results: List of individual rule results.
        row_count: Number of rows in the validated DataFrame.
        column_count: Number of columns in the validated DataFrame.
    """
    name: str
    results: List[RuleResult]
    row_count: int
    column_count: int

    @property
    def passed(self) -> bool:
        """True if every rule passed."""
        return all(r.passed for r in self.results)

    @property
    def pass_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def fail_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def total_rules(self) -> int:
        return len(self.results)

    @property
    def failures(self) -> List[RuleResult]:
        """Return only failed results."""
        return [r for r in self.results if not r.passed]

    def to_dict(self) -> Dict:
        """Serialize the full report to a dictionary."""
        return {
            'name': self.name,
            'passed': self.passed,
            'summary': {
                'total_rules': self.total_rules,
                'passed': self.pass_count,
                'failed': self.fail_count,
                'rows_checked': self.row_count,
                'columns_checked': self.column_count,
            },
            'results': [
                {
                    'rule': r.rule_name,
                    'severity': r.severity,
                    'column': r.column,
                    'details': r.details,
                }
                for r in self.results
            ],
        }

    def print_summary(self) -> None:
        """Print a concise summary to stdout."""
        status = 'PASSED' if self.passed else 'FAILED'
        print(f"\n{'=' * 60}")
        print(f"  Validation: {self.name}")
        print(f"  Status:     {status}")
        print(f"  Rules:      {self.pass_count}/{self.total_rules} passed")
        print(f"  Data:       {self.row_count:,} rows x {self.column_count} columns")
        print(f"{'=' * 60}")

    def print_failures(self) -> None:
        """Print details of failed rules."""
        if not self.failures:
            print("  No failures.")
            return

        print(f"\n  Failures ({self.fail_count}):")
        print(f"  {'-' * 56}")
        for r in self.failures:
            print(f"  FAIL  {r.rule_name}")
            if r.column:
                print(f"        column: {r.column}")
            for key, val in r.details.items():
                print(f"        {key}: {val}")
            print()
