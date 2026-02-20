"""
Data contracts for the VoltMarket Transaction Intelligence Engine.

These schemas are the SINGLE SOURCE OF TRUTH for all sessions.
DO NOT modify without updating COORDINATION.md.

Layer flow: raw transactions -> pipeline output -> analytics output -> visualization input
"""

import polars as pl


# =============================================================================
# LAYER 1: Raw Transactions (Data Generator -> data/raw/transactions.parquet)
# =============================================================================

RAW_TRANSACTION_SCHEMA = {
    "transaction_id": pl.Utf8,
    "timestamp": pl.Datetime("us"),
    "week_number": pl.Int32,           # 1-6
    "country": pl.Utf8,                # BR, MX, CO
    "currency": pl.Utf8,               # BRL, MXN, COP
    "amount": pl.Float64,              # In local currency, 10-500
    "amount_usd": pl.Float64,          # Normalized to USD
    "card_brand": pl.Utf8,             # Visa, Mastercard, Amex
    "card_type": pl.Utf8,              # Credit, Debit, Prepaid
    "issuer_bank": pl.Utf8,            # e.g. BBVA, Itau, Bancolombia
    "status": pl.Utf8,                 # approved, declined
    "decline_reason": pl.Utf8,         # null if approved; insufficient_funds, do_not_honor, etc.
    "merchant_id": pl.Utf8,            # Merchant identifier
    "customer_id": pl.Utf8,            # Customer identifier (for cohort analysis)
    "is_recurring": pl.Boolean,        # Recurring transaction flag
    "hour_of_day": pl.Int32,           # 0-23 extracted from timestamp
}

RAW_OUTPUT_PATH = "data/raw/transactions.parquet"


# =============================================================================
# LAYER 2: Pipeline Output (Pipeline -> data/processed/)
# =============================================================================

# Segmented approval rates - one row per unique segment combination
SEGMENT_SCHEMA = {
    "segment_type": pl.Utf8,           # time_weekly, country, card_brand, issuer, amount_bucket, hour_bucket, composite
    "segment_key": pl.Utf8,            # Human-readable key: "MX|Mastercard|Debit|BBVA"
    "dimension_1": pl.Utf8,            # First dimension value
    "dimension_2": pl.Utf8,            # Second dimension value (nullable)
    "dimension_3": pl.Utf8,            # Third dimension value (nullable)
    "period": pl.Utf8,                 # "week_1", "week_2", ..., "weeks_1_3", "weeks_4_6", "all"
    "total_transactions": pl.Int64,
    "approved_transactions": pl.Int64,
    "declined_transactions": pl.Int64,
    "approval_rate": pl.Float64,       # 0.0 - 1.0
    "total_amount_usd": pl.Float64,
    "approved_amount_usd": pl.Float64,
}

SEGMENT_OUTPUT_PATH = "data/processed/segments.parquet"

# Weekly time series for trend analysis
WEEKLY_TREND_SCHEMA = {
    "week_number": pl.Int32,
    "total_transactions": pl.Int64,
    "approved_transactions": pl.Int64,
    "approval_rate": pl.Float64,
    "total_amount_usd": pl.Float64,
}

WEEKLY_TREND_OUTPUT_PATH = "data/processed/weekly_trends.parquet"


# =============================================================================
# LAYER 3: Analytics Output (Analytics -> data/analytics/)
# =============================================================================

INSIGHT_SCHEMA = {
    "rank": pl.Int32,                  # 1 = highest impact
    "insight_id": pl.Utf8,             # Unique identifier
    "title": pl.Utf8,                  # Short headline
    "description": pl.Utf8,            # Full explanation
    "segment_key": pl.Utf8,            # Which segment is affected
    "baseline_rate": pl.Float64,       # Weeks 1-3 approval rate
    "current_rate": pl.Float64,        # Weeks 4-6 approval rate
    "rate_change": pl.Float64,         # Absolute change (negative = decline)
    "affected_transactions": pl.Int64, # How many txns in this segment
    "estimated_revenue_impact_usd": pl.Float64,  # Estimated monthly $ impact
    "severity": pl.Utf8,              # critical, high, medium, low
}

INSIGHTS_OUTPUT_PATH = "data/analytics/insights.parquet"
INSIGHTS_JSON_PATH = "data/analytics/insights.json"

# Anomaly scores per segment
ANOMALY_SCHEMA = {
    "segment_key": pl.Utf8,
    "segment_type": pl.Utf8,
    "baseline_rate": pl.Float64,
    "current_rate": pl.Float64,
    "rate_change": pl.Float64,
    "z_score": pl.Float64,
    "p_value": pl.Float64,
    "is_anomaly": pl.Boolean,
    "affected_transactions": pl.Int64,
    "estimated_revenue_impact_usd": pl.Float64,
}

ANOMALIES_OUTPUT_PATH = "data/analytics/anomalies.parquet"


# =============================================================================
# CONSTANTS (shared across all sessions)
# =============================================================================

COUNTRIES = {
    "BR": {"currency": "BRL", "weight": 0.50, "fx_rate": 0.20},
    "MX": {"currency": "MXN", "weight": 0.30, "fx_rate": 0.058},
    "CO": {"currency": "COP", "weight": 0.20, "fx_rate": 0.00025},
}

CARD_BRANDS = {"Visa": 0.50, "Mastercard": 0.35, "Amex": 0.15}
CARD_TYPES = {"Credit": 0.60, "Debit": 0.35, "Prepaid": 0.05}

ISSUER_BANKS = {
    "BR": ["Itau", "Bradesco", "Banco do Brasil", "Santander BR", "Nubank", "Caixa", "BTG Pactual", "Inter"],
    "MX": ["BBVA", "Banorte", "Santander MX", "Citibanamex", "HSBC MX", "Scotiabank MX", "Banco Azteca", "Inbursa"],
    "CO": ["Bancolombia", "Davivienda", "Banco de Bogota", "BBVA CO", "Scotiabank CO", "Banco Popular", "Banco de Occidente", "Nequi"],
}

DECLINE_REASONS = [
    "insufficient_funds",
    "do_not_honor",
    "expired_card",
    "invalid_transaction",
    "restricted_card",
    "lost_card",
    "pickup_card",
    "fraud_suspected",
]

AMOUNT_BUCKETS = ["$10-50", "$50-100", "$100-200", "$200-350", "$350-500"]
HOUR_BUCKETS = ["morning_6_12", "afternoon_12_17", "evening_17_20", "night_20_24", "late_night_0_6"]

# Target approval rates for data generation (the "story")
BASELINE_APPROVAL_RATE = 0.82   # Weeks 1-3
DEGRADED_APPROVAL_RATE = 0.64   # Weeks 4-6
