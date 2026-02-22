# Star Schema & Fact Table Design – Production-Level Deep Dive

## Author: Bhuvanesh Bharathi
## Project: ecommerce-data-platform
## Purpose: Dimensional Modeling & Fact Table Engineering Principles

---

# 1. Introduction

Dimensional modeling is the foundation of analytical data warehouses.

The most common and production-proven modeling pattern is:

**Star Schema**

This document explains:

- What a star schema is
- Why it exists
- How fact tables should be designed
- Grain definition
- Additivity concepts
- Performance implications
- Production best practices

---

# 2. What is a Star Schema?

A Star Schema consists of:

- One central **Fact Table**
- Multiple surrounding **Dimension Tables**

The structure resembles a star.

Example:

              dim_customer
                    |
dim_product --- fact_sales --- dim_date
|
dim_store

---

# 3. Why Star Schema Exists

Analytical systems prioritize:

- Fast aggregations
- Simple joins
- BI friendliness
- Predictable query patterns
- Cost efficiency

Normalized OLTP models are not optimized for analytics.

Star schema:

- Reduces join complexity
- Improves query readability
- Improves performance
- Simplifies reporting logic

---

# 4. Fact Tables

Fact tables store:

- Measurable events
- Numerical metrics
- Foreign keys to dimensions

Examples:

- Sales
- Payments
- Clicks
- Impressions
- Transactions
- Risk events

---

## Example: fact_sales

Columns:

- sales_sk (surrogate key)
- order_id (degenerate dimension)
- customer_sk
- product_sk
- date_sk
- quantity
- total_amount

---

# 5. The Most Important Concept: Grain

Before designing a fact table, you must define its **grain**.

Grain answers:

> What does one row represent?

Examples:

- One row per order
- One row per order line
- One row per payment transaction
- One row per daily customer summary

Grain must be:

- Explicit
- Documented
- Immutable

---

## Example Grain Definition

fact_sales grain:

> One row per order line item per transaction.

If grain changes later, your warehouse integrity collapses.

Principal rule:

Never design a fact table without explicitly defining grain.

---

# 6. Types of Facts

## Additive Facts

Can be summed across all dimensions.

Example:
- Revenue
- Quantity
- Payment amount

---

## Semi-Additive Facts

Additive across some dimensions but not time.

Example:
- Account balance
- Inventory level

You cannot sum balances across days.

---

## Non-Additive Facts

Cannot be summed.

Example:
- Ratios
- Percentages
- Averages

These should often be computed, not stored.

---

# 7. Dimension Tables

Dimensions provide context.

They contain:

- Descriptive attributes
- Hierarchies
- Business classifications

Examples:

dim_customer:
- city
- country
- segment

dim_product:
- category
- brand

dim_date:
- year
- quarter
- month

---

# 8. Surrogate Keys in Star Schema

All dimension tables use surrogate keys.

Fact tables reference surrogate keys.

Why?

- Faster joins
- Stable identity
- SCD2 compatibility
- Decoupling from source systems

---

# 9. Fact Table Design Principles

## A. Immutable

Fact tables should be append-only.

Avoid updates unless absolutely required.

---

## B. Narrow and Deep

Fact tables:
- Few columns
- Many rows

Dimension tables:
- Many columns
- Fewer rows

---

## C. No Descriptive Attributes in Fact

Do not store:
- customer_name
- product_category

Those belong in dimensions.

Keep fact lean.

---

## D. Foreign Key Integrity

Each foreign key must resolve to a valid dimension record.

No orphan records.

---

# 10. Degenerate Dimensions

Sometimes business identifiers belong in fact.

Example:

- order_id
- invoice_number

These have no attributes and don’t deserve separate dimension tables.

They are called:

**Degenerate Dimensions**

---

# 11. Indexing Strategy

Index:

- Foreign keys
- Frequently filtered columns
- Partition keys (if applicable)

Avoid over-indexing large fact tables.

---

# 12. Partitioning Strategy (Advanced)

At scale, partition fact tables by:

- Date
- Event timestamp

Benefits:

- Faster filtering
- Reduced scans
- Lower compute cost

Example:
Partition by month or day.

---

# 13. Late Arriving Data

Fact arrives late:
- Must resolve dimension version based on event timestamp.

Dimension arrives late:
- May require backfilling fact.

Star schema must handle both gracefully.

---

# 14. Common Modeling Mistakes

❌ No grain definition  
❌ Storing descriptive attributes in fact  
❌ Using business keys instead of surrogate keys  
❌ Ignoring SCD handling  
❌ Over-normalizing dimensions  
❌ Allowing overlapping SCD intervals

---

# 15. Star Schema vs Snowflake Schema

Snowflake schema:
- Normalized dimensions
- More joins
- Harder for BI

Star schema:
- Denormalized dimensions
- Fewer joins
- Faster reporting
- Easier to understand

Modern analytics typically prefers star schema.

---

# 16. Performance Perspective

Star schema optimizes:

- Hash joins
- Aggregations
- Group by queries
- BI workloads
- Cost-based optimizers

This is why BigQuery, Redshift, Snowflake, Databricks all work well with dimensional models.

---

# 17. How This Applies to Our Project

In ecommerce-data-platform:

- fact_sales → event-level data
- dim_customer → SCD Type 2
- dim_product → descriptive product attributes
- dim_date → time intelligence

This mirrors real-world FinTech and eCommerce systems.

---

# 18. Principal-Level Thinking

Before building any warehouse model, ask:

1. What is the grain?
2. What questions will business ask?
3. Which dimensions change over time?
4. What must remain historically accurate?
5. What will scale to billions of rows?
6. How will partitioning work?
7. How will late data be handled?

Star schema is not just modeling.

It is performance engineering + historical truth preservation.

---

# 19. Key Takeaways

- Fact = measurable event
- Dimension = descriptive context
- Grain must be explicit
- Surrogate keys are mandatory
- SCD2 preserves history
- Facts should be immutable
- Design for scale from day one

---