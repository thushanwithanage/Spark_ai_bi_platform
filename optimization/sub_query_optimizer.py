import sqlglot
from sqlglot import parse_one, exp
from sqlglot.optimizer.eliminate_joins import eliminate_joins
from typing import Set, Tuple, Iterable, Union

def contains_subquery(sql: str) -> bool:
    if not isinstance(sql, str):
        raise ValueError("Invalid SQL input")
    try:
        ast = sqlglot.parse_one(sql)
    except Exception as e:
        raise ValueError(f"SQL parsing failed {e}")
    try:
        subquery = ast.find(exp.Subquery)
        if not subquery:
            return False
        else:
            return True
    
    except Exception:
        return ast

def find_unused_subquery_columns(sql: str) -> Tuple[Set[str], Set[str]]:
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("Invalid SQL input")

    try:
        ast = sqlglot.parse_one(sql)
    except Exception as e:
        raise ValueError(f"SQL parsing failed: {str(e)}")

    try:
        subquery = ast.find(exp.Subquery)
        if not subquery:
            return set(), set()

        inner_select = subquery.find(exp.Select)
        if not inner_select:
            return set(), set()

        # Retrieve subquery columns
        subquery_cols: Set[str] = set()
        for expr in inner_select.expressions:
            try:
                if isinstance(expr, exp.Alias):
                    subquery_cols.add(expr.alias.lower())
                elif isinstance(expr, exp.Column):
                    subquery_cols.add(expr.name.lower())
                elif isinstance(expr, exp.Star):
                    return set(), set()
            except Exception:
                continue

        inner_node_ids = {id(node) for node in inner_select.walk()}

        # Retrieve outer query columns
        outer_refs: Set[str] = set()
        for node in ast.walk():
            try:
                if id(node) in inner_node_ids:
                    continue
                if isinstance(node, exp.Column):
                    outer_refs.add(node.name.lower())
            except Exception:
                continue

        return subquery_cols, outer_refs

    except Exception as e:
        return set(), set()

def remove_unused_subquery_columns(subquery_node: Union[str, exp.Expression],columns_to_remove: Iterable[str]) -> str:

    if not columns_to_remove:
        return subquery_node if isinstance(subquery_node, str) else subquery_node.sql()

    try:
        columns_to_remove = {
            str(c).lower() for c in columns_to_remove if c is not None
        }
    except Exception:
        raise ValueError("Invalid columns to remove input")

    try:
        expression = (
            parse_one(subquery_node)
            if isinstance(subquery_node, str)
            else subquery_node
        )
    except Exception as e:
        raise ValueError(f"SQL parsing failed: {str(e)}")

    try:
        select = expression.find(exp.Select)
        if not select:
            return expression.sql()

        new_expressions = []

        for proj in select.expressions:
            try:
                name = None

                if isinstance(proj, exp.Alias) and proj.alias:
                    name = proj.alias.lower()
                elif isinstance(proj, exp.Column) and proj.name:
                    name = proj.name.lower()

                if name and name in columns_to_remove:
                    continue

                new_expressions.append(proj)

            except Exception:
                new_expressions.append(proj)

        if not new_expressions:
            return expression.sql()

        select.set("expressions", new_expressions)

        return expression.sql()

    except Exception:
        return expression.sql()

def replace_subquery_in_query(original_sql: str, updated_subquery_node: Union[str, exp.Expression]) -> str:
    if not original_sql or not isinstance(original_sql, str):
        raise ValueError("Invalid sql input")

    try:
        outer_ast = parse_one(original_sql)
    except Exception:
        return original_sql

    try:
        original_subquery = outer_ast.find(exp.Subquery)
        if not original_subquery:
            return outer_ast.sql()

        if isinstance(updated_subquery_node, str):
            try:
                updated_subquery_node = parse_one(updated_subquery_node)
            except Exception:
                return outer_ast.sql()

        original_subquery.replace(updated_subquery_node)

        return outer_ast.sql()

    except Exception:
        return outer_ast.sql()

def remove_unused_joins(sql: str) -> str:
    if not sql or not isinstance(sql, str):
        raise ValueError("Invalid SQL input")

    try:
        ast = parse_one(sql)
    except Exception:
        return sql

    try:
        ast = eliminate_joins(ast)
        return ast.sql()
    except Exception:
        return sql