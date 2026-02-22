# Slowly Changing Dimension Type 2 (SCD2) – Deep Dive Design Document

## Author: Bhuvanesh Bharathi
## Project: ecommerce-data-platform
## Purpose: Conceptual and Production-Level Understanding of SCD Type 2 Modeling

---

# 1. Introduction

In analytical systems, business entities such as customers, products, merchants, or accounts change over time.  
These changes must often be preserved for historical reporting, financial accuracy, and machine learning reproducibility.

Slowly Changing Dimension Type 2 (SCD2) is a modeling pattern used in dimensional data warehouses to preserve full historical changes of descriptive attributes.

This document provides a deep architectural understanding of SCD2, surrogate keys, temporal resolution, and production-grade design considerations.

---

# 2. Why Slowly Changing Dimensions Exist

Consider a customer who moves from Chennai to Bangalore.

If we overwrite the old city value, we lose historical truth.

Business questions such as:

- What was revenue by city last year?
- How many customers were in Chennai in 2023?
- What was BNPL risk exposure by geography at transaction time?

cannot be answered correctly if history is overwritten.

SCD2 preserves that historical truth.

---

# 3. What is SCD Type 2?

SCD Type 2 preserves full history by:

1. Expiring the previous version of a record
2. Inserting a new version
3. Maintaining time validity ranges

Instead of updating a row, we version it.

---

## Example

Before change:

| customer_sk | customer_id | city     | start_date  | end_date | is_current |
|-------------|------------|----------|------------|----------|------------|
| 10          | C123       | Chennai  | 2023-01-01 | NULL     | true       |

After change:

| customer_sk | customer_id | city       | start_date  | end_date    | is_current |
|-------------|------------|------------|------------|------------|------------|
| 10          | C123       | Chennai    | 2023-01-01 | 2024-01-01 | false      |
| 25          | C123       | Bangalore  | 2024-01-01 | NULL       | true       |

History is preserved.

---

# 4. Core Columns in SCD2 Tables

Every SCD2 dimension typically contains:

- Business Key (e.g., customer_id)
- Surrogate Key (e.g., customer_sk)
- start_date
- end_date
- is_current
- Audit columns (created_at, updated_at)

---

# 5. Surrogate Key vs Business Key

## Business Key
- Comes from source system
- Has business meaning
- Can change
- Can be reused

Example:
customer_id = "CUST_123"

## Surrogate Key
- Warehouse-generated
- Integer
- Immutable
- No business meaning
- Used for joins

Example:
customer_sk = 101

Fact tables must store surrogate keys, not business keys.

---

# 6. Why Fact Tables Must Use Surrogate Keys

## A. Historical Correctness

In SCD2, a single business key can have multiple versions.

If the fact table stores customer_id, it cannot determine which historical version to join.

If the fact table stores customer_sk, it references the exact dimensional version active at transaction time.

This guarantees temporal correctness.

---

## B. Performance at Scale

Joining on:

- VARCHAR(50) → larger index, slower joins
- INT → smaller index, faster joins

At billions of rows, integer joins significantly outperform string joins.

---

## C. Decoupling from Business System Changes

Business identifiers may:

- Change format
- Be merged
- Be corrected
- Be reassigned

Fact tables should remain immutable and stable.

Surrogate keys isolate the warehouse from upstream volatility.

---

## D. Multi-Source Harmonization

In enterprise systems, customer data may come from:

- CRM
- Payments
- Marketing
- Legacy systems

Surrogate keys unify identity internally.

---

# 7. Temporal Join Resolution

Fact loading must be time-aware, not state-aware.

Incorrect approach:

WHERE is_current = true

Correct approach:
order_date >= start_date
AND order_date < end_date

This ensures the fact references the correct version active at transaction time.

This is called temporal resolution.

---

# 8. Interval Modeling – Inclusive vs Exclusive

## Preferred Approach: Half-Open Interval

[start_date, end_date)

Meaning:
- start_date inclusive
- end_date exclusive

Example:

Old row:
start_date = 2023-01-01  
end_date   = 2024-01-01

New row:
start_date = 2024-01-01  
end_date   = NULL

Join logic:
order_date >= start_date
AND order_date < end_date

This avoids boundary overlaps and precision issues.

---

## Legacy Approach: Inclusive End Date

Some systems use:
BETWEEN start_date AND end_date

This is inclusive on both sides.

To avoid overlap, they subtract one second from end_date.

This is less clean and depends on timestamp precision.

Modern systems prefer half-open intervals.

---

# 9. The Overlap Problem

For any business key:

Effective date ranges must never overlap.

Bad example:

| start_date  | end_date    |
|------------|------------|
| 2023-01-01 | 2024-02-01 |
| 2024-01-15 | NULL       |

This creates ambiguity in fact resolution.

Overlap indicates broken SCD2 logic.

---

# 10. Invariants of a Correct SCD2 System

For each business key:

1. Only one row has is_current = true
2. No overlapping effective date ranges
3. Historical records are immutable
4. Fact resolution is deterministic
5. Late-arriving data resolves correctly

---

# 11. Production-Level Enhancements

Mature systems include:

- Overlap detection queries
- Data quality validation jobs
- Unique constraints on (business_key, start_date)
- Monitoring for multiple current rows
- Idempotent merge logic
- Automated reconciliation

---

# 12. Why SCD2 Matters in Finance & Payments

In regulated systems:

If a customer moves cities, historical revenue must remain attributed to the original city.

Incorrect SCD modeling can lead to:

- Financial misreporting
- Compliance violations
- Broken ML feature reproducibility
- Executive reporting errors

SCD2 is mandatory in serious analytical systems.

---

# 13. Mental Model

Think of SCD2 as:

Version control for business entities.

Each row represents a version.  
Facts point to the correct version at transaction time.

This is temporal data modeling.

---

# 14. Key Takeaways

- Dimensions describe entities.
- Facts describe events.
- Surrogate keys ensure stable joins.
- SCD2 preserves historical truth.
- Temporal joins must use effective dates.
- Overlaps indicate broken modeling.
- Half-open intervals are preferred.

---
