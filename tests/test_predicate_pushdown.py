import pytest
from optimization.predicate_pushdown_optimizer import apply_predicate_pushdown

# Valid SQL
def test_valid_sql_returns_string():
    sql = "SELECT * FROM table1 WHERE col1 = 10"
    result = apply_predicate_pushdown(sql)

    assert isinstance(result, str)
    assert "SELECT" in result

# Predicate pushdown
def test_predicate_pushdown():
    sql = """
        SELECT t.col1 
        FROM (SELECT col1, col2 FROM table1) t
        WHERE t.col1 = 10
    """

    result = apply_predicate_pushdown(sql)
    assert "SELECT col1, col2 FROM table1 WHERE col1 = 10" in result

# Invalid input type
def test_invalid_input_type():
    with pytest.raises(ValueError) as e:
        apply_predicate_pushdown(123)
    
    assert "Invalid SQL input" in str(e.value)

# Invalid SQL syntax
def test_invalid_sql_syntax():
    sql = "SELECT * FROM"

    with pytest.raises(ValueError) as e:
        apply_predicate_pushdown(sql)

    assert "SQL parsing failed" in str(e.value)

# String conversion failure
def test_string_conversion_failure(monkeypatch):

    class BadAST:
        def __str__(self):
            raise Exception("Conversion failed")

    def mock_parse(_):
        return BadAST()

    def mock_pushdown(ast):
        return ast

    monkeypatch.setattr(
        "optimization.predicate_pushdown_optimizer.parse_one",
        mock_parse
    )

    monkeypatch.setattr(
        "optimization.predicate_pushdown_optimizer.pushdown_predicates",
        mock_pushdown
    )

    with pytest.raises(RuntimeError) as e:
        apply_predicate_pushdown("SELECT * FROM table")

    assert "Failed to convert AST to string" in str(e.value)