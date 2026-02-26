"""
Tests for SQLValidator – focusing on the CTE user-filter bypass and related
security enforcement.
"""

import pytest

from ariesql._types import Scope, TablePolicy
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
        scope=Scope.GLOBAL,
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
        scope=Scope.GLOBAL,
        allowed_columns={"employee_id", "amount", "from_date", "to_date"},
    ),
}

BLOCKED_FUNCTIONS: set[str] = {"pg_sleep", "pg_cancel_backend", "pg_terminate_backend"}

USER_ID = 123


@pytest.fixture
def validator() -> SQLValidator:
    return SQLValidator(
        table_policies=TABLE_POLICIES, blocked_functions=BLOCKED_FUNCTIONS
    )


# ---------------------------------------------------------------------------
# 1. The exact bypass query from the bug report
#    The CTE wraps `employee` so the old top-level injector never saw it.
#    Expected: the rewritten SQL must contain `employee.id = 123` inside the
#    CTE body, OR the audit pass raises PermissionError.
# ---------------------------------------------------------------------------

BYPASS_QUERY = """
WITH target AS (
    SELECT employee.id FROM employee
    WHERE employee.first_name = 'Tokuyasu' AND employee.last_name = 'Pesch'
),
current_dept AS (
    SELECT de.employee_id, d.dept_name, de.from_date, de.to_date
    FROM department_employee AS de
    JOIN department AS d ON d.id = de.department_id
    JOIN target AS t ON t.id = de.employee_id
    WHERE CURRENT_DATE BETWEEN de.from_date AND de.to_date
),
latest_dept AS (
    SELECT de.employee_id, d.dept_name, de.from_date, de.to_date,
           ROW_NUMBER() OVER (PARTITION BY de.employee_id ORDER BY de.to_date DESC, de.from_date DESC) AS rn
    FROM department_employee AS de
    JOIN department AS d ON d.id = de.department_id
    JOIN target AS t ON t.id = de.employee_id
)
SELECT COALESCE(c.dept_name, l.dept_name) AS dept_name
FROM target AS t
LEFT JOIN current_dept AS c ON c.employee_id = t.id
LEFT JOIN latest_dept AS l ON l.employee_id = t.id AND l.rn = 1
LIMIT 1
"""

# The original (poorly-written) bypass query with unqualified columns inside
# the CTE WHERE – this must always be rejected, never allowed through.
BYPASS_QUERY_UNQUALIFIED = """
WITH target AS (
    SELECT id FROM employee
    WHERE first_name = 'Tokuyasu' AND last_name = 'Pesch'
),
current_dept AS (
    SELECT de.employee_id, d.dept_name, de.from_date, de.to_date
    FROM department_employee AS de
    JOIN department AS d ON d.id = de.department_id
    JOIN target AS t ON t.id = de.employee_id
    WHERE CURRENT_DATE BETWEEN de.from_date AND de.to_date
),
latest_dept AS (
    SELECT de.employee_id, d.dept_name, de.from_date, de.to_date,
           ROW_NUMBER() OVER (PARTITION BY de.employee_id ORDER BY de.to_date DESC, de.from_date DESC) AS rn
    FROM department_employee AS de
    JOIN department AS d ON d.id = de.department_id
    JOIN target AS t ON t.id = de.employee_id
)
SELECT COALESCE(c.dept_name, l.dept_name) AS dept_name
FROM target AS t
LEFT JOIN current_dept AS c ON c.employee_id = t.id
LEFT JOIN latest_dept AS l ON l.employee_id = t.id AND l.rn = 1
LIMIT 1
"""


def test_bypass_via_cte_is_blocked(validator: SQLValidator):
    """
    The original bypass query used unqualified columns (first_name, last_name)
    inside a CTE WHERE clause. This must be rejected outright — unqualified
    columns in filter conditions that could resolve to user-scoped tables are
    not allowed.
    """
    with pytest.raises(PermissionError, match="Unqualified column"):
        validator.validate_query(BYPASS_QUERY_UNQUALIFIED, current_user_id=USER_ID)


def test_bypass_rewritten_sql_contains_filter_in_cte(validator: SQLValidator):
    """
    A properly-qualified CTE query that wraps `employee` must have the user
    filter injected *inside the CTE body*, not just on the outer SELECT.
    Uses the qualified variant of the bypass query (employee.first_name etc.).
    """
    rewritten = validator.validate_query(BYPASS_QUERY, current_user_id=USER_ID)
    # The filter must appear somewhere before the first occurrence of
    # "current_dept" (i.e. inside the target CTE body).
    target_cte_end = rewritten.find("current_dept")
    cte_body = rewritten[:target_cte_end] if target_cte_end != -1 else rewritten
    assert f"employee.id = {USER_ID}" in cte_body, (
        f"User filter not injected into CTE body:\n{rewritten}"
    )


# ---------------------------------------------------------------------------
# 2. Unqualified column reference to a user-scoped table should be rejected
# ---------------------------------------------------------------------------


def test_unqualified_user_scoped_column_rejected(validator: SQLValidator):
    """
    An unqualified column in a WHERE condition that could resolve to a
    user-scoped table must be rejected to prevent implicit data leakage.
    (Unqualified columns in SELECT projections are allowed.)
    """
    sql = "SELECT employee.first_name FROM employee WHERE id = 999"
    with pytest.raises(PermissionError, match="Unqualified column"):
        validator.validate_query(sql, current_user_id=USER_ID)


# ---------------------------------------------------------------------------
# 3. Simple top-level query – filter injected as before
# ---------------------------------------------------------------------------


def test_simple_select_gets_user_filter(validator: SQLValidator):
    sql = "SELECT employee.first_name, employee.last_name FROM employee"
    rewritten = validator.validate_query(sql, current_user_id=USER_ID)
    assert f"employee.id = {USER_ID}" in rewritten


# ---------------------------------------------------------------------------
# 4. Filter already present must not be duplicated
# ---------------------------------------------------------------------------


def test_existing_filter_not_duplicated(validator: SQLValidator):
    sql = f"SELECT employee.first_name FROM employee WHERE employee.id = {USER_ID}"
    rewritten = validator.validate_query(sql, current_user_id=USER_ID)
    assert rewritten.count(f"employee.id = {USER_ID}") == 1


# ---------------------------------------------------------------------------
# 5. override_user_id respected
# ---------------------------------------------------------------------------


def test_override_user_id_is_used(validator: SQLValidator):
    sql = "SELECT employee.first_name FROM employee"
    rewritten = validator.validate_query(
        sql, current_user_id=USER_ID, override_user_id=999
    )
    assert "employee.id = 999" in rewritten
    assert f"employee.id = {USER_ID}" not in rewritten


# ---------------------------------------------------------------------------
# 6. skip_user_filter_tables skips injection for that table only
# ---------------------------------------------------------------------------


def test_skip_user_filter_tables(validator: SQLValidator):
    sql = "SELECT employee.first_name FROM employee"
    rewritten = validator.validate_query(
        sql, current_user_id=USER_ID, skip_user_filter_tables={"employee"}
    )
    assert f"employee.id = {USER_ID}" not in rewritten


# ---------------------------------------------------------------------------
# 7. skip_user_filter skips all injection
# ---------------------------------------------------------------------------


def test_skip_user_filter_entirely(validator: SQLValidator):
    sql = "SELECT employee.first_name FROM employee"
    rewritten = validator.validate_query(
        sql, current_user_id=USER_ID, skip_user_filter=True
    )
    assert f"employee.id = {USER_ID}" not in rewritten


# ---------------------------------------------------------------------------
# 8. Write operations blocked
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_sql",
    [
        "INSERT INTO employee (first_name) VALUES ('hack')",
        "UPDATE employee SET first_name='hack' WHERE employee.id = 1",
        "DELETE FROM employee WHERE employee.id = 1",
        "DROP TABLE employee",
    ],
)
def test_write_operations_blocked(validator: SQLValidator, bad_sql: str):
    with pytest.raises(PermissionError, match="Only SELECT"):
        validator.validate_query(bad_sql, current_user_id=USER_ID)


# ---------------------------------------------------------------------------
# 9. SELECT * expanded to allowed columns
# ---------------------------------------------------------------------------


def test_select_star_expanded(validator: SQLValidator):
    """Bare SELECT * should be rewritten to the allowed columns."""
    result = validator.validate_query("SELECT * FROM employee", current_user_id=USER_ID)
    # All allowed columns must appear, qualified with the table name
    for col in ("birth_date", "first_name", "gender", "hire_date", "id", "last_name"):
        assert f"employee.{col}" in result, f"Expected employee.{col} in result"
    # The literal '*' must not appear
    assert "*" not in result


def test_select_star_aliased_table(validator: SQLValidator):
    """SELECT * from an aliased table should use the alias as qualifier."""
    result = validator.validate_query(
        "SELECT * FROM employee AS e", current_user_id=USER_ID
    )
    for col in ("birth_date", "first_name", "gender", "hire_date", "id", "last_name"):
        assert f"e.{col}" in result, f"Expected e.{col} in result"
    assert "*" not in result


def test_select_qualified_star(validator: SQLValidator):
    """SELECT e.* should expand only that table's columns."""
    result = validator.validate_query(
        "SELECT e.*, d.dept_name FROM employee AS e JOIN department AS d ON d.id = 1",
        current_user_id=USER_ID,
    )
    # employee columns via alias e
    for col in ("birth_date", "first_name", "gender", "hire_date", "id", "last_name"):
        assert f"e.{col}" in result
    # department column kept as-is
    assert "d.dept_name" in result
    assert "*" not in result


def test_select_star_multi_table_join(validator: SQLValidator):
    """SELECT * from a join should include allowed columns from ALL direct tables."""
    result = validator.validate_query(
        "SELECT * FROM employee AS e JOIN department AS d ON d.id = 1",
        current_user_id=USER_ID,
    )
    # employee columns
    for col in ("birth_date", "first_name", "gender", "hire_date", "id", "last_name"):
        assert f"e.{col}" in result
    # department columns
    for col in ("dept_name", "id"):
        assert f"d.{col}" in result
    assert "*" not in result


# ---------------------------------------------------------------------------
# 10. Disallowed column rejected
# ---------------------------------------------------------------------------


def test_disallowed_column_rejected(validator: SQLValidator):
    # `salary` is not in employee's allowed_columns
    with pytest.raises(PermissionError, match="not allowed"):
        validator.validate_query(
            "SELECT employee.salary FROM employee", current_user_id=USER_ID
        )


# ---------------------------------------------------------------------------
# 11. Table not in manifest rejected
# ---------------------------------------------------------------------------


def test_unknown_table_rejected(validator: SQLValidator):
    with pytest.raises(PermissionError, match="Table not allowed"):
        validator.validate_query(
            "SELECT secret.data FROM secret", current_user_id=USER_ID
        )


# ---------------------------------------------------------------------------
# 12. LIMIT is injected when absent
# ---------------------------------------------------------------------------


def test_limit_injected(validator: SQLValidator):
    sql = "SELECT employee.first_name FROM employee WHERE employee.id = 123"
    rewritten = validator.validate_query(
        sql, current_user_id=USER_ID, skip_user_filter=True
    )
    assert "LIMIT" in rewritten.upper()


def test_custom_limit_respected(validator: SQLValidator):
    sql = "SELECT employee.first_name FROM employee WHERE employee.id = 123"
    rewritten = validator.validate_query(
        sql, current_user_id=USER_ID, skip_user_filter=True, custom_limit=50
    )
    assert "LIMIT 50" in rewritten


# ---------------------------------------------------------------------------
# 13. Blocked function rejected
# ---------------------------------------------------------------------------


def test_blocked_function_rejected(validator: SQLValidator):
    sql = "SELECT pg_sleep(5)"
    with pytest.raises(PermissionError, match="Blocked function"):
        validator.validate_query(sql, current_user_id=USER_ID)


# ---------------------------------------------------------------------------
# 14. CTE with subquery injected filter – deeper nesting
# ---------------------------------------------------------------------------


def test_nested_cte_user_filter_injected(validator: SQLValidator):
    """
    Two-level CTE: inner CTE selects from employee, outer CTE joins the inner.
    User filter must be injected into the inner CTE body.
    """
    sql = """
    WITH inner_cte AS (
        SELECT employee.id, employee.first_name FROM employee
    ),
    outer_cte AS (
        SELECT inner_cte.id, inner_cte.first_name
        FROM inner_cte
    )
    SELECT outer_cte.id, outer_cte.first_name FROM outer_cte
    """
    rewritten = validator.validate_query(sql, current_user_id=USER_ID)
    assert f"employee.id = {USER_ID}" in rewritten


# ---------------------------------------------------------------------------
# 15. Aliased table – filter uses the alias, not the raw table name
# ---------------------------------------------------------------------------


def test_aliased_table_filter_uses_alias(validator: SQLValidator):
    """
    SELECT e.first_name FROM employee AS e
    The injected filter must be ``e.id = <uid>``, not ``employee.id = <uid>``.
    Using the raw table name would cause a PostgreSQL error:
      "invalid reference to FROM-clause entry for table employee"
    """
    sql = "SELECT e.first_name FROM employee AS e"
    rewritten = validator.validate_query(sql, current_user_id=USER_ID)
    assert f"e.id = {USER_ID}" in rewritten
    assert f"employee.id = {USER_ID}" not in rewritten


def test_aliased_table_in_cte_filter_uses_alias(validator: SQLValidator):
    """
    The exact query from the bug report: employee aliased as 'e' inside a CTE.
    Filter must be injected as ``e.id = <uid>`` inside the CTE body.
    """
    sql = """
    WITH persons AS (
        SELECT e.id FROM employee AS e
        WHERE e.first_name = 'Tokuyasu' AND e.last_name = 'Pesch'
    )
    SELECT persons.id FROM persons
    """
    rewritten = validator.validate_query(sql, current_user_id=USER_ID)
    # Alias-aware filter inside the CTE body
    assert f"e.id = {USER_ID}" in rewritten
    # Must NOT use bare table name – that causes a Postgres error
    assert f"employee.id = {USER_ID}" not in rewritten


def test_aliased_table_existing_filter_not_duplicated(validator: SQLValidator):
    """
    If the alias-qualified filter is already present it must not be added twice.
    """
    sql = f"SELECT e.first_name FROM employee AS e WHERE e.id = {USER_ID}"
    rewritten = validator.validate_query(sql, current_user_id=USER_ID)
    assert rewritten.count(f"e.id = {USER_ID}") == 1


# ---------------------------------------------------------------------------
# 16. LLM-injected user_key value must be stripped and replaced
# ---------------------------------------------------------------------------


def test_llm_injected_user_key_is_replaced(validator: SQLValidator):
    """
    The LLM may hardcode a user_id in the WHERE clause (e.g. e.id = 123 when
    the real calling user is 456). The validator must strip whatever value the
    LLM put and replace it with the authoritative current_user_id.
    This prevents the LLM from pre-seeding a value that bypasses the filter.
    """
    # LLM hardcoded e.id = 123 but the calling user is 456
    sql = "SELECT e.first_name FROM employee AS e WHERE e.id = 123"
    rewritten = validator.validate_query(sql, current_user_id=456)
    assert "e.id = 456" in rewritten
    assert "e.id = 123" not in rewritten


def test_llm_injected_user_key_in_cte_is_replaced(validator: SQLValidator):
    """
    Same attack through a CTE: LLM embeds e.id = 123 inside the CTE body,
    but the calling user is 456. The validator must overwrite it with e.id = 456.
    """
    sql = """
    WITH emp AS (
        SELECT e.id FROM employee AS e
        WHERE e.first_name = 'Tokuyasu' AND e.last_name = 'Pesch' AND e.id = 123
    )
    SELECT emp.id FROM emp
    """
    rewritten = validator.validate_query(sql, current_user_id=456)
    assert "e.id = 456" in rewritten
    assert "e.id = 123" not in rewritten


def test_llm_injected_unaliased_user_key_is_replaced(validator: SQLValidator):
    """
    LLM hardcodes employee.id = 123 (unaliased) for a different calling user.
    """
    sql = "SELECT employee.first_name FROM employee WHERE employee.id = 123"
    rewritten = validator.validate_query(sql, current_user_id=456)
    assert "employee.id = 456" in rewritten
    assert "employee.id = 123" not in rewritten


# ---------------------------------------------------------------------------
# Alias-aware column access enforcement
# ---------------------------------------------------------------------------


def test_alias_disallowed_column_blocked(validator: SQLValidator):
    """
    e.salary references the salary column via the alias 'e' for 'employee'.
    'salary' is NOT in employee's allowed_columns so it must be rejected,
    even though the raw qualifier 'e' is not in TABLE_POLICIES.
    """
    sql = "SELECT e.salary FROM employee AS e"
    with pytest.raises(PermissionError, match="salary"):
        validator.validate_query(sql, current_user_id=1)


def test_alias_allowed_column_passes(validator: SQLValidator):
    """
    e.first_name via alias 'e' for 'employee' — first_name IS in allowed_columns.
    Should pass validation without error.
    """
    sql = "SELECT e.first_name, e.last_name FROM employee AS e"
    result = validator.validate_query(sql, current_user_id=1)
    assert "e.id = 1" in result


def test_alias_disallowed_column_in_cte_blocked(validator: SQLValidator):
    """
    Alias-qualified disallowed column inside a CTE body must be rejected.
    """
    sql = """
    WITH emp AS (
        SELECT e.id, e.salary FROM employee AS e
    )
    SELECT emp.id FROM emp
    """
    with pytest.raises(PermissionError, match="salary"):
        validator.validate_query(sql, current_user_id=1)


def test_alias_allowed_column_in_join_passes(validator: SQLValidator):
    """
    JOIN with alias: e.hire_date is in allowed_columns for employee.
    Query should pass and inject e.id = 1 in the JOIN's containing SELECT.
    """
    sql = """
    SELECT e.first_name, d.dept_name
    FROM employee AS e
    JOIN department_employee AS de ON de.employee_id = e.id
    JOIN department AS d ON d.id = de.department_id
    """
    result = validator.validate_query(sql, current_user_id=1)
    assert "e.id = 1" in result
