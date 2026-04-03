import sqlglot
from sqlglot import parse_one, exp
from sqlglot.optimizer.eliminate_joins import eliminate_joins
from typing import Set

def contains_subquery(sql: str):
    ast = sqlglot.parse_one(sql)

    subquery = ast.find(exp.Subquery)
    if not subquery:
        return False
    else:
        return True
    
def find_unused_subquery_columns(sql: str, dialect: str = 'spark') -> Set[str]:
    ast = sqlglot.parse_one(sql, dialect=dialect)

    subquery = ast.find(exp.Subquery)
    if not subquery:
        return set()

    inner_select = subquery.find(exp.Select)
    if not inner_select:
        return set()

    # Retrieve subquery columns 
    subquery_cols: Set[str] = set()
    for expr in inner_select.expressions:
        if isinstance(expr, exp.Alias):
            subquery_cols.add(expr.alias.lower())
        elif isinstance(expr, exp.Column):
            subquery_cols.add(expr.name.lower())
        elif isinstance(expr, exp.Star):
            return set()

    # Retrieve subquery clauses
    clause_cols: dict = {
        "where":    set(),
        "group_by": set(),
        "having":   set(),
        "order_by": set(),
    }

    clause_map = {
        "where":    exp.Where,
        "group_by": exp.Group,
        "having":   exp.Having,
        "order_by": exp.Order,
    }

    for clause_name, clause_type in clause_map.items():
        clause_node = inner_select.find(clause_type)
        if clause_node:
            clause_cols[clause_name].add(str(clause_node))

    inner_node_ids = {id(node) for node in inner_select.walk()}

    # Retrieve outer query columns
    outer_refs: Set[str] = set()
    for node in ast.walk():
        if id(node) in inner_node_ids:
            continue
        if isinstance(node, exp.Column):
            outer_refs.add(node.name.lower())

    return subquery, subquery_cols, outer_refs, clause_cols

def remove_unused_subquery_columns(subquery_node, columns_to_remove):
    if isinstance(subquery_node, str):
        expression = parse_one(subquery_node)
    else:
        expression = subquery_node

    select = expression.find(exp.Select)
    if not select:
        return expression.sql()

    columns_to_remove = {c.lower() for c in columns_to_remove}

    new_expressions = []
    for proj in select.expressions:
        name = None

        if isinstance(proj, exp.Alias):
            name = proj.alias.lower()
        elif isinstance(proj, exp.Column):
            name = proj.name.lower()
        else:
            name = None

        if name is not None and name in columns_to_remove:
            continue

        new_expressions.append(proj)

    if not new_expressions:
        raise ValueError("Cannot remove all columns from SELECT")

    select.set("expressions", new_expressions)
    return expression.sql()

def replace_subquery_in_query(original_sql: str, updated_subquery_node, dialect: str = 'spark') -> str:
    outer_ast = parse_one(original_sql, dialect=dialect)
    
    original_subquery = outer_ast.find(exp.Subquery)
    if not original_subquery:
        raise ValueError("No subquery found in original SQL")

    original_subquery.replace(updated_subquery_node)

    return outer_ast.sql()

def remove_unused_joins(sql: str):
    return eliminate_joins(parse_one(sql))