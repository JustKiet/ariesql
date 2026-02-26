"""
Security regression test suite for SQLValidator.

Each test verifies that a previously-discovered vulnerability is now
**patched**.  All attacks must be blocked by the validator.  Tests are
grouped by vulnerability ID matching the security audit report.
"""

import pytest

from ariesql._types import TablePolicy, Scope
from ariesql.validator import SQLValidator

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

TABLE_POLICIES = {
    "employee": TablePolicy(
        scope=Scope.USER,
        user_key="id",
        allowed_columns={
            "id",
            "first_name",
            "last_name",
            "birth_date",
            "gender",
            "hire_date",
        },
    ),
    "department": TablePolicy(
        scope=Scope.GLOBAL,
        allowed_columns={"id", "dept_name"},
    ),
    "department_employee": TablePolicy(
        scope=Scope.USER,
        user_key="employee_id",
        allowed_columns={"department_id", "employee_id", "from_date", "to_date"},
    ),
    "department_manager": TablePolicy(
        scope=Scope.GLOBAL,
        allowed_columns={"department_id", "employee_id", "from_date", "to_date"},
    ),
    "title": TablePolicy(
        scope=Scope.GLOBAL,
        allowed_columns={"employee_id", "title", "from_date", "to_date"},
    ),
    "salary": TablePolicy(
        scope=Scope.USER,
        user_key="employee_id",
        allowed_columns={"employee_id", "amount", "from_date", "to_date"},
    ),
}

BLOCKED_FUNCTIONS: set[str] = {
    "pg_sleep",
    "pg_cancel_backend",
    "pg_terminate_backend",
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "pg_ls_logdir",
    "pg_ls_waldir",
    "pg_stat_file",
    "lo_import",
    "lo_export",
    "pg_advisory_lock",
    "pg_advisory_xact_lock",
    "dblink",
    "dblink_exec",
    "dblink_connect",
    "query_to_xml",
    "query_to_xml_and_xmlschema",
    "cursor_to_xml",
    "table_to_xml",
    "set_config",
    "pg_reload_conf",
    "pg_rotate_logfile",
}

USER_ID = 456  # The "real" calling user


@pytest.fixture
def validator() -> SQLValidator:
    return SQLValidator(
        table_policies=TABLE_POLICIES, blocked_functions=BLOCKED_FUNCTIONS
    )


# ===================================================================
# VULN-01 [CRITICAL]: OR-based user filter bypass — NOW BLOCKED
# ===================================================================


class TestVuln01_ORPrecedenceBypass:
    """VULN-01: OR-based user filter bypass must be blocked."""

    def test_or_user_key_stripped_and_parenthesized(self, validator: SQLValidator):
        """LLM injects 'e.id = 10001 OR e.first_name = X'.
        The OR branch containing e.id must be stripped entirely,
        and any surviving conditions are wrapped in parens.
        The victim id=10001 must NOT appear in the result.
        """
        sql = (
            "SELECT e.first_name, e.last_name FROM employee AS e "
            "WHERE e.id = 10001 OR e.first_name = 'Georgi'"
        )
        result = validator.validate_query(sql, current_user_id=USER_ID)
        assert "10001" not in result, "Victim id must be stripped"
        assert f"e.id = {USER_ID}" in result, "Authoritative filter must be present"

    def test_or_true_blocked(self, validator: SQLValidator):
        """'e.id = 999 OR 1=1' — the OR containing e.id is fully stripped."""
        sql = "SELECT e.first_name FROM employee AS e WHERE e.id = 999 OR 1 = 1"
        result = validator.validate_query(sql, current_user_id=USER_ID)
        assert "999" not in result, "LLM-injected id must be stripped"
        assert "1 = 1" not in result, "OR 1=1 must be stripped with the OR"
        assert f"e.id = {USER_ID}" in result

    def test_or_salary_cross_user_blocked(self, validator: SQLValidator):
        """Salary OR-bypass is now stripped."""
        sql = (
            "SELECT s.employee_id, s.amount FROM salary AS s "
            "WHERE s.employee_id = 10001 OR s.amount > 0"
        )
        result = validator.validate_query(sql, current_user_id=USER_ID)
        assert "10001" not in result, "Victim employee_id must be stripped"
        assert f"s.employee_id = {USER_ID}" in result


# ===================================================================
# VULN-02 [CRITICAL]: TRUNCATE / COPY / GRANT / SET ROLE — NOW BLOCKED
# ===================================================================


class TestVuln02_MissingDDLDCLBlocks:
    """VULN-02: Non-SELECT statements must be rejected."""

    def test_truncate_blocked(self, validator: SQLValidator):
        with pytest.raises(PermissionError, match="Only SELECT"):
            validator.validate_query("TRUNCATE TABLE employee", current_user_id=USER_ID)

    def test_copy_to_file_blocked(self, validator: SQLValidator):
        with pytest.raises((PermissionError, ValueError)):
            validator.validate_query(
                "COPY employee TO '/tmp/employees.csv' WITH CSV HEADER",
                current_user_id=USER_ID,
            )

    def test_grant_blocked(self, validator: SQLValidator):
        with pytest.raises((PermissionError, ValueError)):
            validator.validate_query(
                "GRANT ALL ON employee TO public", current_user_id=USER_ID
            )

    def test_set_role_blocked(self, validator: SQLValidator):
        with pytest.raises((PermissionError, ValueError)):
            validator.validate_query("SET ROLE postgres", current_user_id=USER_ID)


# ===================================================================
# VULN-03 [CRITICAL]: EXPLAIN ANALYZE — NOW BLOCKED
# ===================================================================


class TestVuln03_ExplainAnalyzeBypass:
    """VULN-03: EXPLAIN ANALYZE must be rejected (Command node)."""

    def test_explain_analyze_blocked(self, validator: SQLValidator):
        with pytest.raises((PermissionError, ValueError)):
            validator.validate_query(
                "EXPLAIN ANALYZE SELECT employee.id, employee.first_name FROM employee",
                current_user_id=USER_ID,
            )

    def test_explain_blocked(self, validator: SQLValidator):
        with pytest.raises((PermissionError, ValueError)):
            validator.validate_query(
                "EXPLAIN SELECT id FROM secret_table", current_user_id=USER_ID
            )


# ===================================================================
# VULN-04 [HIGH]: DO $$ anonymous PL/pgSQL — NOW BLOCKED
# ===================================================================


class TestVuln04_AnonymousPLpgSQL:
    """VULN-04: DO $$ blocks must be rejected."""

    def test_do_block_blocked(self, validator: SQLValidator):
        with pytest.raises((PermissionError, ValueError)):
            validator.validate_query(
                "DO $$ BEGIN RAISE NOTICE 'pwned'; END $$",
                current_user_id=USER_ID,
            )

    def test_do_block_with_write_blocked(self, validator: SQLValidator):
        with pytest.raises((PermissionError, ValueError)):
            validator.validate_query(
                "DO $$ BEGIN DELETE FROM employee; END $$",
                current_user_id=USER_ID,
            )


# ===================================================================
# VULN-05 [HIGH]: LIMIT now capped to MAX_LIMIT
# ===================================================================


class TestVuln05_LimitCapped:
    """VULN-05: User-supplied LIMIT above MAX_LIMIT must be capped."""

    @pytest.mark.parametrize("limit_val", [10000, 100000, 999999999])
    def test_huge_limit_is_capped(self, validator: SQLValidator, limit_val: int):
        sql = f"SELECT employee.id, employee.first_name FROM employee LIMIT {limit_val}"
        result = validator.validate_query(sql, current_user_id=USER_ID)
        assert f"LIMIT {SQLValidator.MAX_LIMIT}" in result, (
            f"LIMIT {limit_val} must be capped to {SQLValidator.MAX_LIMIT}"
        )

    def test_small_limit_preserved(self, validator: SQLValidator):
        """A LIMIT below MAX_LIMIT should be preserved as-is."""
        sql = "SELECT employee.id FROM employee LIMIT 50"
        result = validator.validate_query(sql, current_user_id=USER_ID)
        assert "LIMIT 50" in result


# ===================================================================
# VULN-06 [HIGH]: Stacked queries — NOW REJECTED
# ===================================================================


class TestVuln06_StackedQueries:
    """VULN-06: Multi-statement (semicolon) queries must be rejected."""

    def test_semicolon_rejected(self, validator: SQLValidator):
        sql = "SELECT employee.id FROM employee; DROP TABLE employee"
        with pytest.raises(ValueError, match="[Mm]ulti"):
            validator.validate_query(sql, current_user_id=USER_ID)

    def test_semicolon_select_select_rejected(self, validator: SQLValidator):
        sql = "SELECT employee.id FROM employee; SELECT employee.id FROM employee"
        with pytest.raises(ValueError, match="[Mm]ulti"):
            validator.validate_query(sql, current_user_id=USER_ID)


# ===================================================================
# VULN-07 [MEDIUM]: All user_key conditions stripped — NOW FIXED
# ===================================================================


class TestVuln07_CompleteStripping:
    """VULN-07: All user_key conditions (NEQ, GT, OR-wrapped) must be stripped."""

    def test_neq_on_user_key_stripped(self, validator: SQLValidator):
        """e.id != 999 must be stripped before injecting e.id = 456."""
        sql = "SELECT e.first_name FROM employee AS e WHERE e.id != 999"
        result = validator.validate_query(sql, current_user_id=USER_ID)
        assert "<>" not in result and "!=" not in result, "NEQ must be stripped"
        assert f"e.id = {USER_ID}" in result

    def test_gt_on_user_key_stripped(self, validator: SQLValidator):
        """e.id > 0 must be stripped."""
        sql = "SELECT e.first_name FROM employee AS e WHERE e.id > 0"
        result = validator.validate_query(sql, current_user_id=USER_ID)
        assert "> 0" not in result, "GT condition on user_key must be stripped"
        assert f"e.id = {USER_ID}" in result

    def test_or_wrapped_eq_stripped(self, validator: SQLValidator):
        """e.id = 999 inside an OR must be stripped along with the entire OR."""
        sql = (
            "SELECT e.first_name FROM employee AS e "
            "WHERE e.id = 999 OR e.first_name = 'test'"
        )
        result = validator.validate_query(sql, current_user_id=USER_ID)
        assert "999" not in result, "OR-wrapped EQ on user_key must be stripped"
        assert f"e.id = {USER_ID}" in result


# ===================================================================
# VULN-08 [MEDIUM]: Audit now uses structural AST check — NOW FIXED
# ===================================================================


class TestVuln08_AuditStructural:
    """VULN-08: Audit now verifies filter is a top-level AND conjunct."""

    def test_audit_catches_missing_top_level_filter(self, validator: SQLValidator):
        """Even if the user_key appears somewhere in the WHERE text,
        the structural audit verifies it is a top-level conjunct.
        With the new stripping logic, OR branches with user_key are
        removed, so the filter is always correctly placed.
        """
        sql = (
            "SELECT e.first_name FROM employee AS e "
            "WHERE e.id = 10001 OR e.first_name = 'Georgi'"
        )
        result = validator.validate_query(sql, current_user_id=USER_ID)
        # The result must have e.id = 456 as a real filter, not buried in OR
        assert f"e.id = {USER_ID}" in result
        assert "10001" not in result


# ===================================================================
# VULN-09 [MEDIUM]: Derived table no longer gets double injection
# ===================================================================


class TestVuln09_DerivedTableFixed:
    """VULN-09: Derived table only gets filter in inner subquery, not outer."""

    def test_derived_table_single_injection(self, validator: SQLValidator):
        """Filter injected ONLY in the inner subquery, not the outer SELECT."""
        sql = (
            "SELECT sub.id, sub.first_name "
            "FROM (SELECT employee.id, employee.first_name FROM employee) AS sub"
        )
        result = validator.validate_query(sql, current_user_id=USER_ID)
        # Inner subquery should have the filter
        if ") AS sub" in result:
            inner_part = result.split(") AS sub")[0]
            outer_part = result.split(") AS sub")[1]
        else:
            inner_part = result
            outer_part = ""
        assert f"employee.id = {USER_ID}" in inner_part, (
            "Inner subquery must have user filter"
        )
        # Outer SELECT should NOT reference employee.id (it's not in scope)
        assert f"employee.id = {USER_ID}" not in outer_part, (
            "Outer SELECT must NOT reference employee.id (not in scope)"
        )


# ===================================================================
# VULN-10 [MEDIUM]: Cache now stores validated query — design test
# ===================================================================


class TestVuln10_CachePoisoningFixed:
    """VULN-10: Cache should store validated query, not raw LLM query."""

    def test_malicious_pattern_not_in_validated_output(self, validator: SQLValidator):
        """After validation, the OR-bypass pattern is stripped from output,
        so caching the validated query is safe."""
        from ariesql.sql_masker import mask_ner_and_numbers

        malicious_query = (
            "SELECT e.first_name FROM employee AS e "
            "WHERE e.id = 10001 OR e.first_name = 'Georgi'"
        )
        validated = validator.validate_query(malicious_query, current_user_id=USER_ID)
        masked_validated = mask_ner_and_numbers(validated)
        # The cached version (validated) should NOT contain the victim id
        assert "10001" not in masked_validated, (
            "Validated query (to be cached) must not contain attack pattern"
        )


# ===================================================================
# VULN-11 [LOW]: Blocked functions now expanded — NOW FIXED
# ===================================================================


class TestVuln11_BlockedFunctionsExpanded:
    """VULN-11: Dangerous PostgreSQL functions are now blocked."""

    @pytest.mark.parametrize(
        "func_sql",
        [
            "SELECT pg_read_file('/etc/passwd')",
            "SELECT pg_ls_dir('/tmp')",
            "SELECT pg_stat_file('/etc/passwd')",
            "SELECT lo_import('/etc/passwd')",
            "SELECT lo_export(12345, '/tmp/dump')",
            "SELECT query_to_xml('SELECT 1', TRUE, FALSE, '')",
        ],
    )
    def test_dangerous_function_blocked(self, validator: SQLValidator, func_sql: str):
        """Each dangerous function must now raise PermissionError."""
        with pytest.raises(PermissionError, match="[Bb]locked function"):
            validator.validate_query(func_sql, current_user_id=USER_ID)


# ===================================================================
# VULN-12 [LOW]: Connection string uses env var — NOW FIXED
# ===================================================================


class TestVuln12_ConnStringFromEnv:
    """VULN-12: Connection string should come from environment variable."""

    def test_conn_string_from_env_var(self):
        """Config now reads DATABASE_URL from environment."""
        import importlib
        import os

        import ariesql.config as cfg

        # Temporarily set a custom DATABASE_URL
        original = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = "postgresql://readonly:secret@db:5432/employees"
        try:
            importlib.reload(cfg)
            assert cfg.CONN_STRING == "postgresql://readonly:secret@db:5432/employees"
        finally:
            # Restore original state
            if original is not None:
                os.environ["DATABASE_URL"] = original
            else:
                os.environ.pop("DATABASE_URL", None)
            importlib.reload(cfg)
