# VoltMarket Transaction Intelligence Engine

**Source:** https://yuno-challenge.vercel.app/challenge/cmlv4hdvo000esnxuy7n1vhx4

## Overview

Build a data pipeline and analytics tool to diagnose why VoltMarket's payment approval rates collapsed from 82% to 64% over 6 weeks, costing ~$1.2M monthly. The system must identify hidden patterns in transaction data across multiple dimensions.

## Core Requirements

### Requirement 1: Multi-Dimensional Segmentation Pipeline

- Ingest raw transaction records with approval status and metadata
- Calculate approval rates across: time (hourly/daily/weekly), geography, payment instruments, issuer banks, transaction amounts, decline reasons
- Enable flexible drilling into specific segment combinations

### Requirement 2: Anomaly Detection & Insights

- Compare weeks 1-3 versus weeks 4-6 performance
- Identify statistical outliers relative to overall averages
- Deliver 3-5 ranked insights with quantified impact (segment affected, magnitude of change, estimated revenue impact)
- Example: "Mastercard debit in Mexico dropped 78% to 52% starting Week 4 (~$180K impact)"

### Requirement 3: Visual Intelligence Dashboard or Report

- Display overall approval rate trend showing 82% to 64% decline
- Present breakdown across 3+ major dimensions
- Highlight problem areas clearly
- Format for non-technical payment ops managers
- Generate 2-3 "aha moments" (e.g., "BBVA Mexico is the culprit")

## Test Data Specifications

- 5,000-10,000 transactions across 6 weeks
- Three countries: Brazil (50%), Mexico (30%), Colombia (20%)
- Card brands: Visa (50%), Mastercard (35%), Amex (15%)
- Card types: Credit (60%), Debit (35%), Prepaid (5%)
- 8-12 issuer banks per region
- Amounts: $10-$500 in local currencies
- Embedded degradation patterns:
  - **BBVA Mexico** significant decline weeks 4-6
  - **High-value transactions** (>$200) deteriorate later
  - **Evening transactions** (post-8 PM) underperform

## Stretch Goals

- **Cohort Analysis**: First-time vs returning customers; acquisition period/channel comparisons
- **Predictive Component**: Risk-scoring model based on historical patterns
- **Real-Time Alerting**: System detecting sub-60% approval rate triggers in critical segments

## Deliverables

1. Complete source code (pipeline, analysis, visualization)
2. README with setup, usage, architecture, key findings summary
3. Generated test data or generation script
4. Output artifacts (dashboard screenshots, reports, notebook results)
5. Optional: 2-3 minute demo video

## Evaluation Criteria (100 points)

| Category | Points | Focus |
|----------|--------|-------|
| Data Pipeline Design | 25 | Clean ingestion, transformation, segmentation |
| Analytical Quality | 25 | Meaningful insights, anomaly detection, quantified impact |
| Visualization & Communication | 20 | Clear, actionable presentation |
| Test Data Quality | 10 | Realistic, comprehensive with embedded patterns |
| Code Quality & Documentation | 10 | Organization, README, setup clarity |
| Completeness | 10 | End-to-end working solution meeting core requirements |

## Time Constraint

**2 hours** with AI tool assistance permitted
