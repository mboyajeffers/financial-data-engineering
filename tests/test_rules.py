"""
Tests for validation rules.
"""

import pandas as pd

from src.quality.rules import (
    CompletenessRule,
    UniquenessRule,
    RangeRule,
    PatternRule,
    CustomRule,
    RuleSet,
)


class TestCompletenessRule:

    def test_clean_data_passes(self, clean_df):
        rule = CompletenessRule(columns=['id', 'name'])
        result = rule.evaluate(clean_df)
        assert result.passed is True

    def test_null_detection(self, messy_df):
        rule = CompletenessRule(columns=['id', 'email'])
        result = rule.evaluate(messy_df)
        assert result.passed is False
        assert 'id' in result.details['failures']
        assert result.details['failures']['id']['null_count'] == 1

    def test_threshold_allows_partial(self, messy_df):
        rule = CompletenessRule(columns=['id'], threshold=0.7)
        result = rule.evaluate(messy_df)
        assert result.passed is True  # 4/5 = 0.8 >= 0.7

    def test_missing_column(self, clean_df):
        rule = CompletenessRule(columns=['nonexistent'])
        result = rule.evaluate(clean_df)
        assert result.passed is False

    def test_empty_dataframe(self):
        df = pd.DataFrame({'col': []})
        rule = CompletenessRule(columns=['col'])
        result = rule.evaluate(df)
        assert result.passed is True  # No rows, no nulls


class TestUniquenessRule:

    def test_unique_data_passes(self, clean_df):
        rule = UniquenessRule(columns=['id'])
        result = rule.evaluate(clean_df)
        assert result.passed is True
        assert result.details['duplicate_rows'] == 0

    def test_duplicate_detection(self, messy_df):
        rule = UniquenessRule(columns=['id'])
        result = rule.evaluate(messy_df)
        assert result.passed is False
        assert result.details['duplicate_rows'] == 2

    def test_composite_key(self):
        df = pd.DataFrame({
            'year': [2024, 2024, 2025],
            'quarter': ['Q1', 'Q2', 'Q1'],
        })
        rule = UniquenessRule(columns=['year', 'quarter'])
        result = rule.evaluate(df)
        assert result.passed is True

    def test_missing_column_fails(self, clean_df):
        rule = UniquenessRule(columns=['nonexistent'])
        result = rule.evaluate(clean_df)
        assert result.passed is False
        assert 'error' in result.details


class TestRangeRule:

    def test_within_range_passes(self, clean_df):
        rule = RangeRule(column='score', min_val=0, max_val=100)
        result = rule.evaluate(clean_df)
        assert result.passed is True

    def test_out_of_range_detected(self, messy_df):
        rule = RangeRule(column='score', min_val=0, max_val=100)
        result = rule.evaluate(messy_df)
        assert result.passed is False
        assert result.details['violations'] == 2  # 120 and -5

    def test_min_only(self, financial_df):
        rule = RangeRule(column='revenue', min_val=0)
        result = rule.evaluate(financial_df)
        assert result.passed is True

    def test_max_only(self):
        df = pd.DataFrame({'pct': [0.1, 0.5, 0.99, 1.5]})
        rule = RangeRule(column='pct', max_val=1.0)
        result = rule.evaluate(df)
        assert result.passed is False
        assert result.details['violations'] == 1

    def test_missing_column(self, clean_df):
        rule = RangeRule(column='nonexistent', min_val=0)
        result = rule.evaluate(clean_df)
        assert result.passed is False

    def test_reports_actual_bounds(self, clean_df):
        rule = RangeRule(column='score', min_val=0, max_val=100)
        result = rule.evaluate(clean_df)
        assert result.details['min_found'] == 78
        assert result.details['max_found'] == 95


class TestPatternRule:

    def test_valid_pattern_passes(self, financial_df):
        rule = PatternRule(column='cik', pattern=r'^\d{10}$')
        result = rule.evaluate(financial_df)
        assert result.passed is True

    def test_invalid_pattern_detected(self, messy_df):
        rule = PatternRule(column='email', pattern=r'^[\w.+-]+@[\w-]+\.[\w.]+$')
        result = rule.evaluate(messy_df)
        assert result.passed is False
        assert result.details['mismatches'] >= 1

    def test_ticker_format(self, financial_df):
        rule = PatternRule(column='ticker', pattern=r'^[A-Z]{1,5}$')
        result = rule.evaluate(financial_df)
        assert result.passed is True


class TestCustomRule:

    def test_passing_custom_rule(self, clean_df):
        rule = CustomRule(
            func=lambda df: (len(df) >= 3, {'count': len(df)}),
            name='min_rows',
        )
        result = rule.evaluate(clean_df)
        assert result.passed is True

    def test_failing_custom_rule(self, clean_df):
        rule = CustomRule(
            func=lambda df: (len(df) >= 100, {'count': len(df)}),
            name='min_rows',
        )
        result = rule.evaluate(clean_df)
        assert result.passed is False

    def test_custom_column_check(self, financial_df):
        rule = CustomRule(
            func=lambda df: (
                df['revenue'].sum() > 1_000_000_000_000,
                {'total_revenue': float(df['revenue'].sum())},
            ),
            name='total_revenue_threshold',
            column='revenue',
        )
        result = rule.evaluate(financial_df)
        assert result.passed is True


class TestRuleSet:

    def test_all_pass(self, clean_df):
        rs = RuleSet('clean_checks')
        rs.add(CompletenessRule(columns=['id', 'name']))
        rs.add(UniquenessRule(columns=['id']))
        rs.add(RangeRule(column='score', min_val=0, max_val=100))
        results = rs.evaluate(clean_df)
        assert all(r.passed for r in results)
        assert len(results) == 3

    def test_partial_failure(self, messy_df):
        rs = RuleSet('messy_checks')
        rs.add(CompletenessRule(columns=['name']))  # passes
        rs.add(UniquenessRule(columns=['id']))       # fails
        results = rs.evaluate(messy_df)
        assert results[0].passed is True
        assert results[1].passed is False

    def test_chaining(self, clean_df):
        rs = RuleSet('chain')
        rs.add(CompletenessRule(columns=['id'])).add(RangeRule(column='score', min_val=0))
        assert len(rs) == 2
