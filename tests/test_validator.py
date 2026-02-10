"""
Tests for the DataValidator orchestrator.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from quality import DataValidator, CompletenessRule, UniquenessRule, RangeRule


class TestDataValidator:

    def test_validate_clean_data(self, clean_df):
        v = DataValidator("test")
        v.add_rule(CompletenessRule(columns=['id', 'name']))
        v.add_rule(UniquenessRule(columns=['id']))
        report = v.validate(clean_df)
        assert report.passed is True
        assert report.pass_count == 2
        assert report.fail_count == 0

    def test_validate_messy_data(self, messy_df):
        v = DataValidator("test")
        v.add_rule(CompletenessRule(columns=['id']))
        v.add_rule(UniquenessRule(columns=['id']))
        v.add_rule(RangeRule(column='score', min_val=0, max_val=100))
        report = v.validate(messy_df)
        assert report.passed is False
        assert report.fail_count == 3

    def test_rule_count(self):
        v = DataValidator("test")
        assert v.rule_count == 0
        v.add_rule(CompletenessRule(columns=['a']))
        v.add_rule(CompletenessRule(columns=['b']))
        assert v.rule_count == 2

    def test_add_rules_batch(self, clean_df):
        v = DataValidator("test")
        v.add_rules([
            CompletenessRule(columns=['id']),
            UniquenessRule(columns=['id']),
        ])
        assert v.rule_count == 2

    def test_report_row_and_column_count(self, financial_df):
        v = DataValidator("test")
        v.add_rule(CompletenessRule(columns=['cik']))
        report = v.validate(financial_df)
        assert report.row_count == 5
        assert report.column_count == 5


class TestValidationReport:

    def test_to_dict_structure(self, clean_df):
        v = DataValidator("sec_check")
        v.add_rule(CompletenessRule(columns=['id']))
        report = v.validate(clean_df)
        d = report.to_dict()
        assert d['name'] == 'sec_check'
        assert d['passed'] is True
        assert d['summary']['total_rules'] == 1
        assert d['summary']['rows_checked'] == 5

    def test_failures_list(self, messy_df):
        v = DataValidator("test")
        v.add_rule(CompletenessRule(columns=['id']))
        v.add_rule(UniquenessRule(columns=['id']))
        report = v.validate(messy_df)
        assert len(report.failures) == 2

    def test_print_summary(self, clean_df, capsys):
        v = DataValidator("demo")
        v.add_rule(CompletenessRule(columns=['id']))
        report = v.validate(clean_df)
        report.print_summary()
        captured = capsys.readouterr()
        assert 'PASSED' in captured.out
        assert '1/1 passed' in captured.out

    def test_print_failures_when_none(self, clean_df, capsys):
        v = DataValidator("demo")
        v.add_rule(CompletenessRule(columns=['id']))
        report = v.validate(clean_df)
        report.print_failures()
        captured = capsys.readouterr()
        assert 'No failures' in captured.out
