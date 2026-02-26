import sqlglot
from sqlglot import exp

from ariesql._types import Scope, TablePolicy

TablePolicies = dict[str, TablePolicy]


class SQLValidator:
    MAX_LIMIT = 50

    def __init__(
        self,
        table_policies: TablePolicies,
        blocked_functions: set[str],
        dialect: str = "postgres",
        default_schema: str | None = None,
    ) -> None:
        self._table_policies = table_policies
        self._blocked_functions = blocked_functions
        self._dialect = dialect
        self._default_schema = default_schema

    def _parse_sql(self, sql: str) -> sqlglot.Expression:
        """Parse a single SQL statement.

        Rejects multi-statement input (stacked queries) by using
        ``sqlglot.parse`` and requiring exactly one result.  Also rejects
        any statement that sqlglot could not fully parse (returned as None).
        """
        try:
            statements = sqlglot.parse(sql, dialect=self._dialect)
        except Exception as e:
            raise ValueError(f"Invalid SQL: {e}")

        # Filter out None entries (empty splits from trailing semicolons)
        statements = [s for s in statements if s is not None]

        if len(statements) == 0:
            raise ValueError("Empty SQL statement")
        if len(statements) > 1:
            raise ValueError("Multi-statement (stacked) queries are not allowed")
        return statements[0]

    def _enforce_read_only(self, ast: sqlglot.Expression):
        """Only allow SELECT / UNION queries.

        Uses an **allowlist** approach: the top-level AST node must be an
        ``exp.Select`` or ``exp.Union``.  Everything else — including
        ``exp.Command`` (EXPLAIN, DO $$, SET ROLE …), TRUNCATE, COPY,
        GRANT, REVOKE — is rejected.  Write / DDL nodes are additionally
        checked inside the tree to catch e.g. INSERT inside a CTE.
        """
        # Top-level must be SELECT or UNION (allowlist).
        if not isinstance(ast, (exp.Select, exp.Union)):
            raise PermissionError(
                f"Only SELECT queries are allowed (got {type(ast).__name__})"
            )

        # Defence-in-depth: walk the full tree for write / DDL nodes that
        # could be embedded inside CTEs or subqueries.
        for node in ast.walk():
            if isinstance(
                node,
                (
                    exp.Insert,
                    exp.Update,
                    exp.Delete,
                    exp.Merge,
                    exp.Create,
                    exp.Drop,
                    exp.Alter,
                ),
            ):
                raise PermissionError("Write or DDL operation detected")

    def _enforce_safe_functions(self, ast: sqlglot.Expression):
        for func in ast.find_all(exp.Func):
            if func.name.lower() in self._blocked_functions:
                raise PermissionError(f"Blocked function: {func.name}")

    def _collect_cte_names(self, ast: sqlglot.Expression) -> set[str]:
        """Return the set of CTE alias names defined anywhere in the query."""
        cte_names: set[str] = set()
        for cte in ast.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(cte.alias)
        return cte_names

    def _extract_tables(self, ast: sqlglot.Expression) -> set[str]:
        """
        Return every *real* table name referenced anywhere in the expression
        (including inside CTE bodies), excluding the CTE aliases themselves.
        """
        cte_names = self._collect_cte_names(ast)
        tables: set[str] = set()
        for table in ast.find_all(exp.Table):
            if table.name not in cte_names:
                tables.add(table.name)
        return tables

    def _direct_tables_of_select(self, select: exp.Select) -> dict[str, str]:
        """
        Return a mapping of real_table_name → qualifier for tables that are
        *directly* in the FROM / JOIN clauses of a single exp.Select node
        (does not recurse into subqueries). CTE aliases are excluded.

        The qualifier is the alias when one is present (e.g. ``employee AS e``
        → ``"e"``), otherwise the table name itself.  This is used to build
        WHERE predicates with the correct table reference so that aliased tables
        like ``SELECT e.id FROM employee AS e`` receive ``e.id = <uid>`` rather
        than the invalid ``employee.id = <uid>``.
        """
        # Collect CTE aliases from the root statement
        root = select
        while root.parent is not None:
            root = root.parent
        cte_names = self._collect_cte_names(root)

        tables: dict[str, str] = {}

        def _add(table: exp.Table) -> None:
            if table.name in cte_names:
                return
            qualifier = table.alias if table.alias else table.name
            tables[table.name] = qualifier

        def _collect_direct_tables(node: exp.Expression) -> None:
            """Walk *node* for Table nodes but **stop** at subquery
            boundaries (Select / Subquery) so we only pick up tables
            that are directly in this FROM / JOIN, not inside derived
            tables or lateral subqueries."""
            for child in node.iter_expressions():
                if isinstance(child, (exp.Select, exp.Subquery)):
                    continue  # don't descend into subqueries
                if isinstance(child, exp.Table):
                    _add(child)
                else:
                    _collect_direct_tables(child)

        # FROM clause – sqlglot stores this under the key "from_"
        from_clause = select.args.get("from_")
        if from_clause:
            _collect_direct_tables(from_clause)

        # JOIN clauses – stored under "joins"
        for join in select.args.get("joins") or []:
            _collect_direct_tables(join)

        return tables

    def _enforce_table_access(self, ast: sqlglot.Expression):
        tables = self._extract_tables(ast)
        for table in tables:
            if table not in self._table_policies:
                raise PermissionError(f"Table not allowed: {table}")

    def _enforce_column_access(self, ast: sqlglot.Expression):
        """
        Enforce column-level access policy.

        Raises PermissionError for:
        - Qualified columns (table.column or alias.column) that are outside the
          allowed set for the resolved real table.
        - Unqualified columns whose name matches a restricted column on any
          user-scoped table that is referenced in the same query, preventing
          ambiguous column references from leaking scoped data.

        Alias resolution: for each SELECT node, a qualifier→real_table map is
        built from ``_direct_tables_of_select`` (inverted: qualifier → table_name)
        so that alias-qualified references like ``e.salary`` are correctly
        resolved to the ``employee`` table policy rather than silently ignored.
        """
        cte_names = self._collect_cte_names(ast)

        # Build a map: column_name -> set of user-scoped table names that own it
        user_scoped_columns: dict[str, set[str]] = {}
        for tname, policy in self._table_policies.items():
            if policy.scope == Scope.USER:
                for col in policy.allowed_columns:
                    user_scoped_columns.setdefault(col, set()).add(tname)

        referenced_real_tables = self._extract_tables(ast)

        for col in ast.find_all(exp.Column):
            table = col.table
            column = col.name

            if table and table in cte_names:
                # e.g. cte_alias.column_name — the column comes from a CTE output,
                # not directly from a real table. Safe to skip enforcement here;
                # the CTE body itself was already validated above.
                continue

            if table:
                # Could be a real table name OR an alias for a real table.
                # First try a direct policy lookup (covers un-aliased references).
                real_table = table
                policy = self._table_policies.get(table)

                if policy is None:
                    # Not a known real table name — try to resolve via alias map.
                    # Walk up to the enclosing SELECT and ask _direct_tables_of_select
                    # for its qualifier→real_table mapping.
                    enclosing_select: exp.Select | None = None
                    node = col.parent
                    while node is not None:
                        if isinstance(node, exp.Select):
                            enclosing_select = node
                            break
                        node = node.parent

                    if enclosing_select is not None:
                        # _direct_tables_of_select returns {real_name: qualifier}.
                        # Invert to {qualifier: real_name}.
                        direct = self._direct_tables_of_select(enclosing_select)
                        qualifier_to_real = {v: k for k, v in direct.items()}
                        real_table = qualifier_to_real.get(table, table)
                        policy = self._table_policies.get(real_table)

                if policy is None:
                    # Still unresolved (e.g. subquery column, unknown qualifier).
                    continue

                if column not in policy.allowed_columns:
                    raise PermissionError(
                        f"Column '{column}' not allowed on table '{real_table}'"
                        + (f" (via alias '{table}')" if real_table != table else "")
                    )
            else:
                # Completely unqualified column reference (no table prefix at all).
                # Projections (direct child of exp.Select) are safe — the column
                # is just being selected from whatever table is in scope.
                # Dangerous are unqualified columns in WHERE / JOIN ON / HAVING
                # conditions, which could silently bypass a user-scoped filter.

                # Climb parent chain to find if we're inside a filter context.
                in_filter_context = False
                node = col.parent
                while node is not None:
                    if isinstance(node, (exp.Where, exp.Having, exp.Join)):
                        in_filter_context = True
                        break
                    if isinstance(node, exp.Select):
                        break  # reached SELECT boundary without a filter node
                    node = node.parent

                if not in_filter_context:
                    continue  # unqualified projection – safe

                owning_tables = user_scoped_columns.get(column, set())
                conflicting = owning_tables & referenced_real_tables
                if conflicting:
                    raise PermissionError(
                        f"Unqualified column '{column}' is ambiguous: it could "
                        f"resolve to user-scoped table(s) {sorted(conflicting)}. "
                        "Always qualify columns with their table name."
                    )

    def _expand_select_star(self, ast: sqlglot.Expression) -> None:
        """Rewrite every ``SELECT *`` and ``SELECT t.*`` into explicit
        column lists derived from the table policies.

        This is safer than blocking ``*`` outright because:
        - Only columns present in the policy's ``allowed_columns`` are emitted.
        - Queries that naturally use ``*`` succeed instead of erroring.
        - Defence-in-depth: even if ``_enforce_column_access`` had a bug,
          disallowed columns are never in the projection.

        Stars that reference a CTE alias (rather than a real table) are left
        untouched because the CTE's output schema is not described by the
        table policies — the CTE body itself is already validated separately.
        """
        cte_names = self._collect_cte_names(ast)

        for select in ast.find_all(exp.Select):
            direct = self._direct_tables_of_select(select)
            # Invert: qualifier → real_table_name
            qualifier_to_real = {v: k for k, v in direct.items()}

            new_expressions: list[exp.Expression] = []
            changed = False

            for expr in select.expressions:
                # Case 1: bare SELECT * (Star is direct child of Select)
                if isinstance(expr, exp.Star):
                    if not direct:
                        # No resolvable tables (e.g. SELECT * FROM cte_only)
                        # — keep the star as-is; column access check will
                        # catch disallowed columns if any slip through.
                        new_expressions.append(expr)
                        continue
                    changed = True
                    for real_table, qualifier in direct.items():
                        policy = self._table_policies.get(real_table)
                        if policy is None:
                            continue
                        for col_name in sorted(policy.allowed_columns):
                            new_expressions.append(
                                exp.Column(
                                    this=exp.Identifier(this=col_name, quoted=False),
                                    table=exp.Identifier(this=qualifier, quoted=False),
                                )
                            )

                # Case 2: qualified SELECT t.* (Column node whose `this` is Star)
                elif (
                    isinstance(expr, exp.Column)
                    and isinstance(expr.this, exp.Star)
                    and expr.table
                ):
                    table_ref = expr.table
                    # If it references a CTE alias, leave it alone.
                    if table_ref in cte_names:
                        new_expressions.append(expr)
                        continue
                    # Resolve qualifier → real table name
                    real_table = qualifier_to_real.get(table_ref, table_ref)
                    policy = self._table_policies.get(real_table)
                    if policy is None:
                        # Unknown qualifier — keep as-is and let column
                        # access enforcement catch anything bad.
                        new_expressions.append(expr)
                        continue
                    changed = True
                    for col_name in sorted(policy.allowed_columns):
                        new_expressions.append(
                            exp.Column(
                                this=exp.Identifier(this=col_name, quoted=False),
                                table=exp.Identifier(this=table_ref, quoted=False),
                            )
                        )

                # Case 3: any other expression — keep as-is
                else:
                    new_expressions.append(expr)

            if changed:
                select.set("expressions", new_expressions)

    def _strip_user_key_conditions(
        self,
        select: exp.Select,
        user_key: str,
        qualifier: str,
    ) -> None:
        """
        Remove **all** existing conditions that reference
        ``qualifier.user_key`` from the WHERE clause of *select*.

        This prevents a caller (or the LLM) from pre-seeding any kind of
        user_key predicate — equality, inequality, OR-wrapped, IN, BETWEEN,
        comparisons, LIKE, IS, etc.

        The implementation walks the full WHERE tree:
        - AND nodes: recurse into both branches; prune any branch that
          touches the user_key.
        - OR nodes: if *either* branch references the user_key, the
          **entire** OR is removed.  This is the safe choice because
          keeping one OR branch alive could widen the result set beyond
          what the authoritative filter intends.
        - Leaf comparisons: any node whose subtree contains a Column
          matching ``qualifier.user_key`` is removed.
        """
        where = select.args.get("where")
        if not where:
            return

        def _references_user_key(node: exp.Expression) -> bool:
            """Return True if *node* contains a Column for the user_key."""
            for col in node.find_all(exp.Column):
                if col.name == user_key and col.table == qualifier:
                    return True
            return False

        def _strip(node: exp.Expression) -> exp.Expression | None:
            """
            Recursively remove any branch that references the user_key.
            Returns None when the entire expression should be removed.
            """
            if isinstance(node, exp.And):
                left = _strip(node.left)
                right = _strip(node.right)
                if left is None:
                    return right
                if right is None:
                    return left
                node.set("this", left)
                node.set("expression", right)
                return node

            if isinstance(node, exp.Or):
                # If either branch of an OR touches the user_key, drop the
                # whole OR — keeping the other branch could widen results.
                if _references_user_key(node.left) or _references_user_key(node.right):
                    return None
                # Neither branch touches user_key — keep as-is.
                return node

            # For every other node type (EQ, NEQ, GT, LT, GTE, LTE,
            # Between, Like, Is, In, …): remove if it references user_key.
            if _references_user_key(node):
                return None

            return node

        new_condition = _strip(where.this)
        if new_condition is None:
            select.set("where", None)
        else:
            select.set("where", exp.Where(this=new_condition))

    def _inject_predicate_into_select(
        self,
        select: exp.Select,
        predicate: exp.Expression,
        user_key: str,
        qualifier: str,
    ) -> None:
        """
        Strip any existing user_key conditions then inject the authoritative
        WHERE predicate into a single exp.Select node.

        The existing WHERE clause (after stripping) is **wrapped in
        parentheses** before combining with AND to prevent SQL operator-
        precedence issues (OR in the original WHERE would otherwise take
        lower precedence than the injected AND, creating a bypass).
        """
        # Always remove whatever the LLM (or caller) put for this user_key first.
        self._strip_user_key_conditions(select, user_key, qualifier)

        where = select.args.get("where")
        if where:
            # Wrap the existing condition in Paren so that:
            #   (original_condition) AND user_filter
            # rather than the dangerous:
            #   original_condition AND user_filter
            # where OR inside original_condition could bypass the filter.
            select.set(
                "where",
                exp.Where(
                    this=exp.And(this=exp.Paren(this=where.this), expression=predicate)
                ),
            )
        else:
            select.set("where", exp.Where(this=predicate))

    def _inject_user_filters(
        self,
        ast: sqlglot.Expression,
        current_user_id: int,
        skip_tables: set[str] | None = None,
        enforce_user_filter_on_global_tables: bool = False,
    ) -> None:
        """
        Inject user-scoped WHERE predicates into **every** SELECT subquery that
        directly references a user-scoped table in its FROM/JOIN clause.

        This covers:
        - Top-level SELECT statements
        - CTE body SELECTs  (the main bypass vector)
        - Correlated / derived subqueries

        Args:
            ast: The full SQL AST to mutate in-place.
            current_user_id: The user ID whose filter to inject.
            skip_tables: Real table names whose filter injection should be skipped.
            enforce_user_filter_on_global_tables: If True, also inject filters for GLOBAL tables (normally only USER-scoped tables are filtered)
        """
        skip_tables = skip_tables or set()

        # Collect all real user-scoped tables present anywhere in the query.
        all_real_tables = self._extract_tables(ast)
        user_scoped_present = {
            t
            for t, p in self._table_policies.items()
            if p.scope == Scope.USER and t in all_real_tables and t not in skip_tables
        }

        if enforce_user_filter_on_global_tables:
            user_scoped_present.update(
                t
                for t, p in self._table_policies.items()
                if p.scope == Scope.GLOBAL
                and t in all_real_tables
                and t not in skip_tables
            )

        if not user_scoped_present:
            return

        # Walk every SELECT node in the tree (top-level and nested).
        for select in ast.find_all(exp.Select):
            direct = self._direct_tables_of_select(select)
            for table_name in user_scoped_present:
                if table_name not in direct:
                    continue
                policy = self._table_policies[table_name]
                if not policy.user_key:
                    continue
                qualifier = direct[table_name]  # alias if present, else table name
                predicate = exp.EQ(
                    this=exp.Column(
                        this=exp.Identifier(this=policy.user_key, quoted=False),
                        table=exp.Identifier(this=qualifier, quoted=False),
                    ),
                    expression=exp.Literal.number(current_user_id),
                )
                self._inject_predicate_into_select(
                    select, predicate, policy.user_key, qualifier
                )

    def _audit_user_filters(
        self,
        ast: sqlglot.Expression,
        current_user_id: int,
        skip_tables: set[str] | None = None,
        enforce_user_filter_on_global_tables: bool = False,
    ) -> None:
        """
        Post-injection audit: verify that every SELECT that directly touches a
        user-scoped table has the expected predicate as a **top-level AND
        conjunct** in its WHERE clause.

        Uses structural AST verification (not string matching) so that a
        predicate buried inside an OR branch does not satisfy the check.

        Raises PermissionError if any such SELECT is missing the filter.
        """
        skip_tables = skip_tables or set()
        all_real_tables = self._extract_tables(ast)

        def _is_top_level_conjunct(
            where: exp.Where, user_key: str, qualifier: str, uid: int
        ) -> bool:
            """
            Return True if the WHERE clause contains an EQ predicate
            ``qualifier.user_key = uid`` that is reachable from the root
            of the WHERE tree by following only AND branches (i.e. it is a
            top-level conjunct, not buried inside an OR).
            """

            def _match_eq(node: exp.Expression) -> bool:
                if not isinstance(node, exp.EQ):
                    return False
                lhs = node.left
                if not isinstance(lhs, exp.Column):
                    return False
                if lhs.name != user_key or lhs.table != qualifier:
                    return False
                rhs = node.right
                try:
                    return int(rhs.this) == uid
                except (ValueError, TypeError, AttributeError):
                    return False

            def _walk_and(node: exp.Expression) -> bool:
                """Recursively search AND-connected conjuncts only."""
                if _match_eq(node):
                    return True
                if isinstance(node, exp.And):
                    return _walk_and(node.left) or _walk_and(node.right)
                # Paren wrapper: unwrap and continue.
                if isinstance(node, exp.Paren):
                    return _walk_and(node.this)
                return False

            return _walk_and(where.this)

        for select in ast.find_all(exp.Select):
            direct = self._direct_tables_of_select(select)
            for table_name, policy in self._table_policies.items():
                if policy.scope != Scope.USER:
                    if (
                        not enforce_user_filter_on_global_tables
                        and policy.scope == Scope.GLOBAL
                    ):
                        continue
                if table_name not in all_real_tables:
                    continue
                if table_name in skip_tables:
                    continue
                if table_name not in direct:
                    continue

                qualifier = direct[table_name]
                where = select.args.get("where")
                if not where or not _is_top_level_conjunct(
                    where, policy.user_key, qualifier, current_user_id
                ):
                    expected_sql = f"{qualifier}.{policy.user_key} = {current_user_id}"
                    raise PermissionError(
                        f"Security violation: SELECT on user-scoped table '{table_name}' "
                        f"is missing the required user filter ({expected_sql}). "
                        "Access denied."
                    )

    def _enforce_limit(self, ast: sqlglot.Expression, max_limit: int | None = None):
        """
        Enforce a LIMIT clause on the query.

        If no LIMIT is present, inject one set to *max_limit* (or MAX_LIMIT).
        If a LIMIT is already present but exceeds the cap, **reduce it** to
        the cap value.  This prevents DoS via resource exhaustion.

        Args:
            ast: The SQL AST to modify
            max_limit: Optional custom max limit (defaults to MAX_LIMIT constant)
        """
        limit_value = max_limit if max_limit is not None else self.MAX_LIMIT

        existing = ast.args.get("limit")
        if existing:
            # Cap existing LIMIT if it exceeds the allowed maximum.
            try:
                current = int(existing.expression.this)
                if current > limit_value:
                    ast.set(
                        "limit",
                        exp.Limit(expression=exp.Literal.number(limit_value)),
                    )
            except (ValueError, TypeError, AttributeError):
                # Cannot determine current limit — replace with safe default.
                ast.set(
                    "limit",
                    exp.Limit(expression=exp.Literal.number(limit_value)),
                )
        else:
            ast.set("limit", exp.Limit(expression=exp.Literal.number(limit_value)))

    def _qualify_tables_with_schema(self, ast: sqlglot.Expression) -> None:
        """
        Add the default schema prefix to every real table reference in the AST.

        Only applies when ``self._default_schema`` is set.  CTE aliases are
        left unqualified so they don't turn into ``schema.cte_name``.  Tables
        that already carry an explicit ``db`` (catalog / schema) are also
        skipped.
        """
        if not self._default_schema:
            return

        cte_names = self._collect_cte_names(ast)

        for table in ast.find_all(exp.Table):
            # Skip CTE references — they aren't real tables.
            if table.name in cte_names:
                continue
            # Skip tables that already have a schema/catalog qualifier.
            if table.args.get("db"):
                continue
            # Only qualify tables we know about (from the manifest).
            if table.name in self._table_policies:
                table.set(
                    "db",
                    exp.Identifier(this=self._default_schema, quoted=False),
                )

    def validate_query(
        self,
        sql: str,
        current_user_id: int,
        skip_user_filter: bool = False,
        skip_user_filter_tables: set[str] | None = None,
        enforce_user_filter_on_global_tables: bool = False,
        override_user_id: int | None = None,
        custom_limit: int | None = None,
    ) -> str:
        """
        Validate and rewrite SQL query with security policies.

        Args:
            sql: The SQL query to validate
            current_user_id: The current user's ID
            skip_user_filter: If True, skips user filter injection entirely
            skip_user_filter_tables: Set of specific tables to skip user filter injection
            enforce_user_filter_on_global_tables: If True, enforces user filter on GLOBAL tables as well (normally only USER-scoped tables are filtered)
            override_user_id: If provided, uses this user_id instead of current_user_id for filters
            custom_limit: If provided, uses this limit instead of the default MAX_LIMIT

        Returns:
            The validated and rewritten SQL query
        Raises:
            ValueError: If the SQL is invalid
            PermissionError: If the SQL violates any security policies
        """
        try:
            ast = self._parse_sql(sql)

            self._enforce_read_only(ast)
            self._enforce_safe_functions(ast)
            self._enforce_table_access(ast)
            self._expand_select_star(ast)
            self._enforce_column_access(ast)

            if not skip_user_filter:
                user_id = (
                    override_user_id
                    if override_user_id is not None
                    else current_user_id
                )
                self._inject_user_filters(
                    ast,
                    user_id,
                    skip_tables=skip_user_filter_tables,
                    enforce_user_filter_on_global_tables=enforce_user_filter_on_global_tables,
                )
                # Strict post-injection audit – raises if any sub-select slipped through.
                self._audit_user_filters(
                    ast,
                    user_id,
                    skip_tables=skip_user_filter_tables,
                    enforce_user_filter_on_global_tables=enforce_user_filter_on_global_tables,
                )

            self._enforce_limit(ast, max_limit=custom_limit)

            # Schema-qualify table references (e.g. for MSSQL where tables
            # live under a named schema like "employees.employee").
            self._qualify_tables_with_schema(ast)

            return ast.sql(dialect=self._dialect)
        except ValueError as ve:
            raise ValueError(f"SQL validation error: {ve}")
        except PermissionError as pe:
            raise PermissionError(f"SQL permission error: {pe}")
