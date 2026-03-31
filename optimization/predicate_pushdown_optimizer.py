from sqlglot import parse_one
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates

def apply_predicate_pushdown(sql: str) -> str:
    ast = parse_one(sql)
    sql = pushdown_predicates(ast)
    return str(sql)