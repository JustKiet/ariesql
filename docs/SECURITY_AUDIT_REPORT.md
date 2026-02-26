# ArieSQL Security Audit Report

**Date:** 2026-02-24  
**Auditor:** GitHub Copilot (Database Security Expert)  
**Scope:** `SQLValidator`, `execute_query` tool, agent pipeline, SQL cache  
**Test Suite:** `tests/test_security_audit.py` (29 PoC tests — all passing)

---

## Executive Summary

A comprehensive security audit of the ArieSQL SQL Agent application uncovered **12 vulnerabilities** across the validator, tool execution layer, and architectural design. Three are rated **CRITICAL** — they allow a malicious or manipulated LLM to exfiltrate data belonging to other users, execute arbitrary DDL/DCL statements, or bypass all security checks entirely.

| Severity | Count |
|----------|-------|
| 🔴 CRITICAL | 3 |
| 🟠 HIGH | 3 |
| 🟡 MEDIUM | 4 |
| 🔵 LOW | 2 |

---

## Vulnerability Details

---

### VULN-01 🔴 CRITICAL — OR-Based User Filter Bypass via SQL Operator Precedence

**Component:** `SQLValidator._strip_user_key_conditions`, `_inject_predicate_into_select`  
**CVSS Estimate:** 9.1 (Critical)  
**Impact:** Complete cross-user data exfiltration  

#### Description

The `_strip_user_key_conditions` method only removes bare `EQ` (`=`) and `IN` conditions on the user_key column. When the LLM generates a WHERE clause with an `OR` branch containing the user_key (e.g., `e.id = <victim_id> OR <condition>`), the OR-wrapped EQ is **not stripped**.

The authoritative filter is then appended with `AND`:

```sql
-- LLM-generated:
WHERE e.id = 10001 OR e.first_name = 'Georgi'

-- After injection (user=456):
WHERE e.id = 10001 OR e.first_name = 'Georgi' AND e.id = 456
```

Due to SQL operator precedence (`AND` binds tighter than `OR`), this evaluates as:

```sql
WHERE e.id = 10001 OR (e.first_name = 'Georgi' AND e.id = 456)
```

**Result:** Any row with `id = 10001` is returned regardless of the user filter. The attacker can read **any user's data** including salary records.

#### Proof of Concept

```python
sql = "SELECT e.first_name, e.last_name FROM employee AS e WHERE e.id = 10001 OR e.first_name = 'Georgi'"
result = validator.validate_query(sql, current_user_id=456)
# Result: "...WHERE e.id = 10001 OR e.first_name = 'Georgi' AND e.id = 456..."
# id=10001 data is leaked to user 456
```

#### Remediation

1. **Wrap the entire existing WHERE clause in parentheses** before appending the authoritative filter:
   ```python
   # Instead of:
   exp.And(this=where.this, expression=predicate)
   # Use:
   exp.And(this=exp.Paren(this=where.this), expression=predicate)
   ```
   This ensures `(e.id = 10001 OR e.first_name = 'Georgi') AND e.id = 456`, which correctly filters.

2. **Strip ALL conditions referencing the user_key** (not just EQ/IN) — recursively walk all nodes including OR branches and remove any comparison involving the user_key column.

3. **Replace the string-based audit** (VULN-08) with a structural AST check that verifies the user_key predicate is at the top-level AND, not buried inside an OR.

---

### VULN-02 🔴 CRITICAL — TRUNCATE / COPY / GRANT / SET ROLE Not Blocked

**Component:** `SQLValidator._enforce_read_only`  
**CVSS Estimate:** 9.8 (Critical)  
**Impact:** Data destruction, privilege escalation, filesystem access  

#### Description

`_enforce_read_only` checks for `Insert`, `Update`, `Delete`, `Merge`, `Create`, `Drop`, `Alter` — but **misses** several dangerous statement types:

| Statement | Risk | sqlglot AST Type |
|-----------|------|-------------------|
| `TRUNCATE TABLE employee` | Wipes all data without logging | Not matched |
| `COPY employee TO '/tmp/dump'` | Dumps table to server filesystem | Not matched |
| `GRANT ALL ON employee TO public` | Escalates privileges | Not matched |
| `REVOKE` | Revokes critical permissions | Not matched |
| `SET ROLE postgres` | Impersonates another DB user | Parsed as `Command` |

#### Proof of Concept

```python
validator.validate_query("TRUNCATE TABLE employee", current_user_id=123)
# Returns: "TRUNCATE TABLE employee" — no error raised

validator.validate_query("COPY employee TO '/tmp/employees.csv' WITH CSV HEADER", current_user_id=123)
# Returns: "COPY employee TO '/tmp/employees.csv' WITH (CSV HEADER)" — no error raised
```

#### Remediation

1. **Adopt an allowlist approach** instead of a blocklist: only allow `exp.Select` (and optionally `exp.Union`) at the top level. Reject everything else.
   ```python
   def _enforce_read_only(self, ast: sqlglot.Expression):
       if not isinstance(ast, (exp.Select, exp.Union)):
           raise PermissionError("Only SELECT queries are allowed")
   ```

2. Additionally, check for `Command` nodes (sqlglot's fallback) and reject them, since any statement sqlglot can't fully parse is likely dangerous.

---

### VULN-03 🔴 CRITICAL — EXPLAIN ANALYZE Bypasses All Security Checks

**Component:** `SQLValidator._parse_sql`, all enforcement methods  
**CVSS Estimate:** 8.6 (High)  
**Impact:** Full security bypass, data exfiltration without user filters  

#### Description

`EXPLAIN ANALYZE SELECT ...` is parsed by sqlglot as a generic `Command` node (not a `Select`). Since `Command` has no `Table`, `Column`, `Select`, or `Func` child nodes:

- `_enforce_read_only` → no write nodes found → ✅ passes
- `_enforce_table_access` → no Table nodes → ✅ passes  
- `_enforce_column_access` → no Column nodes → ✅ passes
- `_inject_user_filters` → no Select nodes → **no filter injected**
- `_enforce_limit` → no limit arg → limit set on Command wrapper (useless)

The inner SELECT executes on PostgreSQL **without any user filter**, exposing all rows.

#### Proof of Concept

```python
result = validator.validate_query(
    "EXPLAIN ANALYZE SELECT employee.id, employee.first_name FROM employee",
    current_user_id=123,
)
# Result: "EXPLAIN ANALYZE SELECT employee.id, employee.first_name FROM employee"
# No "employee.id = 123" filter — returns data for ALL employees
```

#### Remediation

1. **Block EXPLAIN/EXPLAIN ANALYZE entirely** — they expose query plans and execution statistics that leak information about table sizes, indexes, and data distribution.
   ```python
   def _enforce_read_only(self, ast: sqlglot.Expression):
       if isinstance(ast, exp.Command):
           raise PermissionError(f"Statement type not allowed: {ast.this}")
   ```

2. If EXPLAIN must be supported, parse the inner SELECT separately and apply all security checks to it.

---

### VULN-04 🟠 HIGH — DO $$ Anonymous PL/pgSQL Code Execution

**Component:** `SQLValidator._parse_sql`  
**CVSS Estimate:** 8.1 (High)  
**Impact:** Arbitrary code execution within PostgreSQL  

#### Description

`DO $$ BEGIN ... END $$` blocks are parsed as `Command` nodes by sqlglot. Like EXPLAIN, they bypass all security checks. Unlike EXPLAIN, they allow **arbitrary PL/pgSQL code execution**, including writes, filesystem access, and outbound network calls.

#### Proof of Concept

```python
validator.validate_query("DO $$ BEGIN DELETE FROM employee; END $$", current_user_id=123)
# Returns: "DO $$ BEGIN DELETE FROM employee; END $$" — passes all checks
```

#### Remediation

Same as VULN-03: reject all `Command` nodes, or maintain a strict allowlist of permitted top-level AST types.

---

### VULN-05 🟠 HIGH — LIMIT Not Capped (DoS via Resource Exhaustion)

**Component:** `SQLValidator._enforce_limit`  
**CVSS Estimate:** 6.5 (Medium)  
**Impact:** Denial of service, memory exhaustion  

#### Description

`_enforce_limit` only injects a default LIMIT when none is present. If the LLM (or user) supplies `LIMIT 999999999`, it is accepted without capping. With 300K+ rows in the employee table, this can cause memory exhaustion on the application server.

#### Proof of Concept

```python
result = validator.validate_query(
    "SELECT employee.id FROM employee LIMIT 999999999", current_user_id=123
)
# Result contains "LIMIT 999999999" — not capped to MAX_LIMIT (1000)
```

#### Remediation

```python
def _enforce_limit(self, ast: sqlglot.Expression, max_limit: int | None = None):
    limit_value = max_limit if max_limit is not None else self.MAX_LIMIT
    existing_limit = ast.args.get("limit")
    if existing_limit:
        # Cap the existing limit
        try:
            current = int(existing_limit.expression.this)
            if current > limit_value:
                ast.set("limit", exp.Limit(expression=exp.Literal.number(limit_value)))
        except (ValueError, AttributeError):
            ast.set("limit", exp.Limit(expression=exp.Literal.number(limit_value)))
    else:
        ast.set("limit", exp.Limit(expression=exp.Literal.number(limit_value)))
```

---

### VULN-06 🟠 HIGH — Stacked Queries Silently Dropped

**Component:** `SQLValidator._parse_sql` (uses `sqlglot.parse_one`)  
**CVSS Estimate:** 7.2 (High)  
**Impact:** Defense-in-depth failure; potential for second-statement execution  

#### Description

`sqlglot.parse_one` only parses the **first** SQL statement. Any content after a semicolon is silently discarded. While the current code executes `validated_query` (which only contains the first statement), this is a defense-in-depth failure:

- The validator should **reject** multi-statement input rather than silently dropping statements.
- If any future code path accidentally passes the raw query to psycopg2, `cursor.execute()` will execute **all** statements.

#### Proof of Concept

```python
result = validator.validate_query(
    "SELECT employee.id FROM employee; DROP TABLE employee",
    current_user_id=123,
)
# Returns: "SELECT employee.id FROM employee WHERE employee.id = 123 LIMIT 1000"
# "DROP TABLE employee" was silently discarded — no error raised
```

#### Remediation

```python
def _parse_sql(self, sql: str) -> sqlglot.Expression:
    if ";" in sql.replace("'", "").replace('"', ''):
        raise ValueError("Multi-statement queries are not allowed")
    # ... or use sqlglot.parse() and reject if len > 1
    statements = sqlglot.parse(sql, dialect=self._dialect)
    if len(statements) != 1:
        raise ValueError(f"Expected 1 statement, got {len(statements)}")
    return statements[0]
```

---

### VULN-07 🟡 MEDIUM — Incomplete User-Key Condition Stripping

**Component:** `SQLValidator._strip_user_key_conditions`  
**CVSS Estimate:** 5.3 (Medium)  
**Impact:** Conflicting predicates, potential filter bypass in edge cases  

#### Description

`_strip_user_key_conditions` only handles `EQ` (`=`) and `IN` conditions. Other comparison operators on the user_key column survive stripping:

- `e.id != 999` → survives → combined with `AND e.id = 456` produces conflicting predicates
- `e.id > 0` → survives → semantically harmless but indicates incomplete logic
- `e.id BETWEEN 1 AND 100000` → survives → could widen result set

While most of these are neutralized by the injected `AND e.id = <user>`, they represent defense-in-depth gaps.

#### Remediation

Strip **all** comparison predicates where the left-hand side is the user_key column, not just EQ/IN. This includes: `NEQ`, `GT`, `GTE`, `LT`, `LTE`, `BETWEEN`, `LIKE`, `IS`.

---

### VULN-08 🟡 MEDIUM — Audit Check Uses String Containment Instead of AST Verification

**Component:** `SQLValidator._audit_user_filters`  
**CVSS Estimate:** 5.9 (Medium)  
**Impact:** False-positive audit pass on OR-bypass queries  

#### Description

The audit method checks:
```python
if expected_predicate not in where_sql:
    raise PermissionError(...)
```

This is a **substring match**. As long as `e.id = 456` appears anywhere in the WHERE clause text, the audit passes — even if the predicate is inside a nested OR that makes it ineffective (as in VULN-01).

#### Remediation

Replace string matching with a structural AST check: walk the WHERE clause tree and verify that the user_key predicate is a **direct conjunct** (connected by AND at the top level), not buried inside an OR or subexpression.

---

### VULN-09 🟡 MEDIUM — Derived Table Filter Injection Produces Invalid SQL

**Component:** `SQLValidator._inject_user_filters`  
**CVSS Estimate:** 4.3 (Medium)  
**Impact:** SQL runtime errors (availability issue), potential data leak if error is swallowed  

#### Description

When a user-scoped table is accessed via a derived table (subquery in FROM), the filter is injected into **both** the inner subquery and the outer SELECT. The outer WHERE references `employee.id` but only `sub` is in scope:

```sql
SELECT sub.id FROM (SELECT employee.id FROM employee WHERE employee.id = 123) AS sub 
WHERE employee.id = 123  -- ❌ 'employee' not in outer FROM
```

This causes a PostgreSQL runtime error. While this is a "fail-closed" scenario (data isn't leaked), it degrades availability and indicates the filter injection logic doesn't correctly scope table references.

#### Remediation

In `_inject_user_filters`, when processing a `SELECT` node, only inject predicates for tables that are **directly** in the `FROM`/`JOIN` of that specific SELECT — which `_direct_tables_of_select` already does. The bug is that derived subquery aliases (like `sub`) are not recognized as shadowing the inner table name. The outer SELECT should not receive a filter for `employee` since `employee` is not directly in its FROM clause.

---

### VULN-10 🟡 MEDIUM — SQL Cache Poisoning

**Component:** `tool.py` (`execute_query`), `sql_cache.py`  
**CVSS Estimate:** 4.7 (Medium)  
**Impact:** Teaching attack patterns to future LLM invocations  

#### Description

After a successful query execution, the **raw LLM-generated query** (masked for NER) is stored in Redis:

```python
sql_bank.set_sql(masked_query, mask_ner_and_numbers(query))
```

If the LLM generates a query with a malicious pattern (e.g., the OR-bypass from VULN-01), that pattern is stored and will be suggested as a "Similar SQL Query" for future requests with similar intent. This creates a **self-reinforcing attack loop** where one successful exploit teaches the cache the attack pattern.

#### Remediation

1. Store the **validated** query (post-rewrite) rather than the raw LLM-generated query.
2. Or, store the masked query key → validated query mapping:
   ```python
   sql_bank.set_sql(masked_query, mask_ner_and_numbers(validated_query))
   ```

---

### VULN-11 🔵 LOW — Incomplete Blocked Functions List

**Component:** `config.py` (BLOCKED_FUNCTIONS), `SQLValidator._enforce_safe_functions`  
**CVSS Estimate:** 3.7 (Low)  
**Impact:** Filesystem read/write, large object manipulation  

#### Description

Only 3 functions are blocked: `pg_sleep`, `pg_cancel_backend`, `pg_terminate_backend`. Many dangerous PostgreSQL admin functions are unblocked:

| Function | Risk |
|----------|------|
| `pg_read_file()` | Read arbitrary server files |
| `pg_ls_dir()` | List server directories |
| `pg_stat_file()` | Get file metadata |
| `lo_import()` / `lo_export()` | Import/export large objects via filesystem |
| `query_to_xml()` | Execute arbitrary SQL as XML |
| `dblink_exec()` | Execute SQL on remote servers |

#### Remediation

1. **Use an allowlist approach** for functions: only permit known-safe aggregate/scalar functions.
2. Or significantly expand the blocklist to include all `pg_*` admin functions, `lo_*`, `dblink_*`, and `query_to_*`.

---

### VULN-12 🔵 LOW — Hardcoded Superuser Database Credentials

**Component:** `config.py`  
**CVSS Estimate:** 3.1 (Low — assumes network isolation)  
**Impact:** Any validator bypass executes with full superuser privileges  

#### Description

```python
CONN_STRING = "postgresql://postgres:postgres@localhost:5432/employees"
```

The application connects as the `postgres` superuser with a trivial password. Any query that bypasses the validator executes with full superuser privileges, including the ability to drop databases, read filesystem, and modify system catalogs.

#### Remediation

1. Create a **dedicated read-only PostgreSQL role** with minimal privileges:
   ```sql
   CREATE ROLE sql_agent_reader LOGIN PASSWORD '<strong_password>';
   GRANT CONNECT ON DATABASE employees TO sql_agent_reader;
   GRANT USAGE ON SCHEMA employees TO sql_agent_reader;
   GRANT SELECT ON ALL TABLES IN SCHEMA employees TO sql_agent_reader;
   ```
2. Use environment variables for credentials (not hardcoded).
3. Enable `statement_timeout` and `idle_in_transaction_session_timeout` on the role.

---

## Remediation Priority Matrix

| Priority | Vulnerability | Effort | Impact |
|----------|--------------|--------|--------|
| 🔴 P0 | VULN-01: OR-precedence bypass | Medium | Complete data exfiltration |
| 🔴 P0 | VULN-02: Missing DDL/DCL blocks | Low | Data destruction / privesc |
| 🔴 P0 | VULN-03: EXPLAIN ANALYZE bypass | Low | Full security bypass |
| 🟠 P1 | VULN-04: DO $$ block execution | Low | Arbitrary code execution |
| 🟠 P1 | VULN-06: Stacked queries silently dropped | Low | Defense-in-depth failure |
| 🟠 P1 | VULN-05: LIMIT not capped | Low | DoS via resource exhaustion |
| 🟡 P2 | VULN-08: Audit string matching | Medium | Audit evasion |
| 🟡 P2 | VULN-07: Incomplete stripping | Medium | Filter conflicts |
| 🟡 P2 | VULN-09: Derived table invalid SQL | Medium | Availability |
| 🟡 P2 | VULN-10: Cache poisoning | Low | Attack persistence |
| 🔵 P3 | VULN-11: Incomplete function blocklist | Low | Info disclosure |
| 🔵 P3 | VULN-12: Hardcoded superuser creds | Medium | Defense-in-depth |

---

## Architectural Recommendations

### 1. Allowlist-First Design
Replace blocklist approaches with allowlists throughout:
- **Statement types:** Only allow `exp.Select` and `exp.Union` (reject `Command`, all DDL/DCL)
- **Functions:** Maintain an allowlist of safe functions rather than blocking known-dangerous ones
- **Top-level AST validation:** Reject any AST that isn't a recognized safe type

### 2. Predicate Injection Hardening
- Always wrap existing WHERE clauses in parentheses before appending `AND user_filter`
- Strip ALL conditions on the user_key column, not just EQ/IN
- Use structural AST verification for the audit check

### 3. Defense-in-Depth at the Database Layer
- Use a read-only PostgreSQL role with `SELECT`-only grants
- Set `statement_timeout` to prevent long-running queries
- Use `pg_hba.conf` to restrict connections to the application server
- Enable PostgreSQL audit logging

### 4. Input Sanitization
- Reject multi-statement queries (semicolons)
- Reject `EXPLAIN`, `DO`, `COPY`, `SET`, `GRANT`, `REVOKE`, `TRUNCATE` at the parser stage
- Cap user-supplied LIMIT to MAX_LIMIT

### 5. Cache Integrity  
- Store validated (rewritten) queries in the cache, not raw LLM output
- Add TTL and eviction policies to the Redis cache
- Consider signing cached entries to prevent external tampering

---

*Report generated by automated security audit. All 29 PoC tests are in `tests/test_security_audit.py`.*
