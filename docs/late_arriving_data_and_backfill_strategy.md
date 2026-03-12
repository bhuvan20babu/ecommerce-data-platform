# Late Arriving Data & Backfill Strategy – Production Data Engineering

## Author: Bhuvanesh Bharathi
## Project: ecommerce-data-platform
## Purpose: Handling Temporal Corrections & Data Arrival Delays in Analytical Systems

---

# 1. Introduction

In real-world systems, data does not always arrive in order.

Common scenarios:

- A transaction from 3 days ago arrives today.
- A customer profile update arrives after related sales.
- Upstream system delays cause out-of-order ingestion.
- Historical data must be backfilled.

A mature data platform must handle:

- Late arriving facts
- Late arriving dimensions
- Backfills
- Deterministic reprocessing

Without breaking historical accuracy.

---

# 2. Late Arriving Facts

A late arriving fact is:

> A transaction or event that arrives after its actual event time.

Example:

Event occurred:
2024-01-10

Arrives in pipeline:
2024-01-15

---

## Why This Is Dangerous

If dimension resolution uses:
WHERE is_current = true


The fact may attach to the wrong SCD2 version.

Correct resolution must use:

event_timestamp >= start_date
AND event_timestamp < end_date


Fact loading must always be time-aware.

---

# 3. Late Arriving Dimensions

A late arriving dimension occurs when:

- Fact is loaded first
- Dimension record arrives later

Example:

Order exists for customer C123.
Customer dimension record does not yet exist.

---

## Strategies to Handle This

### Strategy 1: Reject Fact
- Log error
- Retry later
- Not scalable in real systems

### Strategy 2: Create "Unknown" Placeholder Record

Insert dimension row:

| customer_sk | customer_id | city     | start_date | end_date | is_current |
|------------|------------|----------|------------|----------|------------|
| 0          | UNKNOWN    | UNKNOWN  | 1900-01-01 | NULL     | true       |

Fact temporarily attaches to surrogate key 0.

Later:
Reprocess and correct mapping.

This is common in enterprise systems.

---

### Strategy 3: Backfill Fact After Dimension Arrival

If late dimension arrives:
- Identify affected fact rows
- Re-resolve surrogate keys
- Update fact table

Requires idempotent pipeline design.

---

# 4. Backfill Strategy

Backfill means:

> Reprocessing historical data to correct or populate missing records.

Common triggers:

- Logic correction
- SCD bug fix
- Schema change
- Historical migration
- Audit requirement

---

# 5. Types of Backfill

## A. Full Backfill

Reprocess entire dataset.

Pros:
- Simple
- Clean

Cons:
- Expensive
- Slow at scale

---

## B. Incremental Backfill

Reprocess only affected time window.

Example:
Reprocess last 30 days.

Better for large systems.

---

## C. Targeted Backfill

Reprocess specific business keys.

Example:
Reprocess only customer C123 history.

Requires fine-grained pipeline control.

---

# 6. Idempotency

A production-grade pipeline must be idempotent.

Definition:

> Running the same job multiple times produces the same result.

This prevents:

- Duplicate facts
- Duplicate SCD2 records
- Broken aggregates

Techniques:

- Use MERGE statements
- Use deterministic surrogate resolution
- Use natural keys + timestamps
- Maintain batch identifiers

---

# 7. Deterministic Fact Resolution

Fact load must:

1. Identify business key
2. Identify event timestamp
3. Resolve correct surrogate key
4. Insert fact

Resolution must not depend on:
- Current state
- Execution time
- Load time

It must depend on:
Event time.

---

# 8. Reprocessing & Temporal Integrity

When reprocessing historical data:

We must ensure:

- No SCD2 overlaps are introduced
- Facts still point to correct historical version
- No double counting occurs

This is why SCD2 logic must be:

- Deterministic
- Version-aware
- Validated

---

# 9. Audit & Reconciliation

Mature systems include:

- Row count reconciliation
- Hash total validation
- Historical consistency checks
- Late-arrival monitoring dashboards

Example metrics:

- % of late facts
- Average delay
- Dimension resolution failures
- Overlap detection alerts

---

# 10. Enterprise Reality

In payments, FinTech, or regulated systems:

Late data is normal.

Examples:

- Chargebacks
- Refunds
- Settlement adjustments
- Regulatory corrections

Your platform must support:

- Retroactive correction
- Immutable audit trail
- Historical reproducibility

---

# 11. Principal-Level Perspective

A data platform must answer:

- Can we re-run history safely?
- Can we audit a specific day from 2 years ago?
- Can we explain why a number changed?
- Can we reconstruct historical state exactly?

If not, the platform is not production-grade.

---

# 12. Recommended Practices for This Project

For ecommerce-data-platform:

1. Use half-open interval logic in SCD2.
2. Resolve surrogate keys using event timestamp.
3. Design Spark jobs to be idempotent.
4. Log unresolved dimension lookups.
5. Implement overlap detection queries.
6. Enable selective backfills.

---

# 13. Key Takeaways

- Data does not always arrive in order.
- Fact resolution must be time-aware.
- Pipelines must be idempotent.
- Backfills are normal, not exceptional.
- Determinism is critical.
- Historical truth must be preserved.

---
