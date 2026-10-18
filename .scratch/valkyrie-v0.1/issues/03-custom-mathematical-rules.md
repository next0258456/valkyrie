# 03: Cross-Column & Nested List Rules (custom_rules)

**What to build:** Allow data engineers to declare intra-row mathematical and relational business logic in `custom_rules`, including cross-column arithmetic comparisons and nested list aggregations (such as summing child item prices inside an order row). Custom rules run within the same single-pass SQL execution plan.

**Blocked by:** 01: Tracer Bullet - Core Single-Pass Contract Engine (not_null, range)

**Status:** completed

- [x] Parsing supports `custom_rules` list with `name`, `expression`, and optional `error_code`.
- [x] Cross-column arithmetic expressions (e.g., `discount_amount <= unit_price * quantity`) are compiled into negated boolean conditions in the `CASE WHEN` clause.
- [x] Nested array functions supported by DuckDB (e.g., `order_total == list_sum(line_items.price)`) are correctly embedded into the query.
- [x] Custom rules default to `ERR_CUSTOM_RULE_FAILED` when no custom `error_code` is explicitly provided.
- [x] Records failing custom rules are properly routed to DLQ with their custom error code and the rule name assigned to `_error_column`.
- [x] End-to-end black-box test verifies validation on nested list/struct data and multi-column arithmetic conditions.
