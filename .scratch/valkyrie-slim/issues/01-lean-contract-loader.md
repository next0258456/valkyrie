# 01: Lean Contract Loader with Strict Whitelisting & Schema Enforcement

**What to build:** Enforce strict root-level Whitelist (`ALLOWED_ROOT_KEYS`) and column-level Whitelist (`ALLOWED_COLUMN_KEYS`) in `contract.py`. Reject empty or missing `schema` (`schema: []`) with `ValueError`. Reject empty or whitespace-only expressions in `custom_rules`.

**Blocked by:** None (can start immediately)

**Status:** completed

- [x] Rejects any unknown or prose root keys outside allowed whitelist (`version`, `dataset`, `owner`, `policies`, `schema`, `custom_rules`).
- [x] Rejects any unknown column keys outside allowed whitelist (`name`, `type`, `not_null`, `unique`, `range`, `enum`, `pattern`, `quality`).
- [x] Raises `ValueError` if `schema` is missing or empty list.
- [x] Raises `ValueError` if `custom_rules` contains an empty `expression`.
