from sqlglot import parse_one
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates

def apply_predicate_pushdown(sql: str) -> str:
    if not isinstance(sql, str):
        raise ValueError("Invalid SQL input")
    
    try:
        ast = parse_one(sql)
    except Exception as e:
        raise ValueError(f"SQL parsing failed {str(e)}")
    try:
        optimized_ast  = pushdown_predicates(ast)
    except Exception:
        return str(ast)
    try:
        return str(optimized_ast)
    except Exception as e:
        raise RuntimeError(f"Failed to convert AST to string {str(e)}") 
    