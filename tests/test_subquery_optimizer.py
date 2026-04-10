import pytest
from optimization.sub_query_optimizer import contains_subquery, find_unused_subquery_columns

# Valid Subquery
def test_valid_subquery_returns_true():
    sql = "SELECT col1 FROM (SELECT col1, col2 FROM table1) AS t"
    result = contains_subquery(sql)

    assert isinstance(result, bool)
    assert result==True

# Non Subquery syntax
def test_non_subquery_returns_false():
    sql = "SELECT col1 FROM table1"
    result = contains_subquery(sql)

    assert isinstance(result, bool)
    assert result==False

# Invalid input type
def test_invalid_input_type():
    with pytest.raises(ValueError) as e:
        contains_subquery(123)
    
    assert "Invalid SQL input" in str(e.value)

# Invalid SQL syntax
def test_invalid_sql():
    sql = "SELECT * FROM"
    with pytest.raises(ValueError) as e:
        contains_subquery(sql)
    
    assert "SQL parsing failed" in str(e.value)

# Subquery in WHERE condition
def test_subquery_in_where_returns_true():
    sql = "SELECT col1 FROM table1 WHERE col2 IN (SELECT col2 FROM table2)"
    result = contains_subquery(sql)

    assert isinstance(result, bool)
    assert result==True

# Subquery in JOINS condition
def test_subquery_in_join_returns_true():
    sql = "SELECT t1.col1  FROM table1 t1 JOIN (SELECT col1 FROM table2) t2 ON t1.col1 = t2.col1"
    result = contains_subquery(sql)

    assert isinstance(result, bool)
    assert result==True

# Empty string input
def test_empty_string_input():
    sql = ""
    with pytest.raises(ValueError) as e:
        contains_subquery(sql)
    
    assert "SQL parsing failed" in str(e.value)

# Whitespaces input
def test_whitespaces_input():
    sql = "    "
    with pytest.raises(ValueError) as e:
        contains_subquery(sql)

    assert "SQL parsing failed" in str(e.value)

# Valid unused columns
def test_valid_unused_columns():
    sql = "SELECT col1 FROM (SELECT col1, col2 FROM table1) AS t"
    subquery_cols, outer_cols = find_unused_subquery_columns(sql)

    assert len(subquery_cols) > 0
    assert isinstance(subquery_cols, set)
    assert len(outer_cols) > 0
    assert isinstance(outer_cols, set)