# Governed AI Self-Service BI Platform

An AI-powered Business Intelligence platform that enables users to query curated business datasets using natural language. The system converts user questions into validated Spark SQL queries and executes them securely against governed analytics datasets.

This project demonstrates how modern analytics platforms combine **AI, data governance, and distributed data processing** to enable safe and scalable self-service analytics.

------------------------------------------------------------------------

## Project Overview

Traditional BI workflows require business users to depend on data teams to write SQL queries or build dashboards.

This platform enables users to ask questions such as:

> **Top 5 revenue generating regions in 2023**

The system automatically:

1.  Converts the question into SQL using an LLM
2.  Validates the generated SQL for safety and governance
3.  Executes the query in Spark
4.  Returns the results to the user

The architecture ensures that AI-generated queries remain **controlled, secure, and aligned with the data model**.

------------------------------------------------------------------------

## Architecture

    User Question
          │
          ▼
    Natural Language → SQL Engine
          │
          ▼
    SQL Validation Layer
          │
          ├── Valid → Execute in Spark
          │
          └── Invalid
                │
                ▼
         AI Query Correction
                │
                ▼
            Re-validation
                │
                ▼
         Results Returned to User

------------------------------------------------------------------------

### Key Design Principles

-   Semantic layer driven analytics
-   AI-assisted query generation
-   Governed SQL validation
-   Distributed query execution using Spark

------------------------------------------------------------------------

## AI Query Validation Pipeline

AI-generated SQL queries pass through a multi-stage validation pipeline before execution.
This architecture ensures that natural language queries remain **safe, governed, and cost-efficient** when executed in the Spark environment.

The pipeline performs the following validation steps:

1.  **SQL Extraction**

    -   Extracts valid SQL statements from the LLM response.
    -   Removes markdown formatting or explanatory text.

2.  **Syntax Parsing**

    -   SQL queries are parsed using `sqlglot` to ensure valid SQL syntax.
    -   Prevents execution of malformed queries.

3.  **Semantic Layer Validation**

    -   Ensures the query only references **approved tables and columns** defined in the semantic layer.
    -   Prevents hallucinated tables or metrics from the AI model.

4.  **Security Validation**

    -   Blocks destructive SQL operations such as:
        -   `DROP`
        -   `DELETE`
        -   `UPDATE`
    -   Ensures queries remain read-only.

5.  **Query Cost Protection**

    -   Enforces safeguards to prevent expensive queries:
        -   Maximum JOIN count
        -   Mandatory `LIMIT`
        -   Maximum result size (`LIMIT ≤ 100`)
        -   `WHERE` filter requirement

6.  **Query Logging**

    -   Logs each query execution with a unique `query_id`, timestamp, user question, generated SQL, validation status, and execution time.
    -   Enables monitoring, auditing, and debugging of AI-generated queries.

7.  **AI Query Correction Loop**

    -   AI models may occasionally generate SQL that fails validation due to syntax errors, missing clauses, or invalid table references.
    -   Instead of immediately failing the request, the system sends the generated SQL and validation error message back to the AI model to produce a corrected query.

    Example validation error:

    > Unbalanced parentheses

    -   The corrected SQL is then re-validated before execution.
    -   A retry limit is enforced to prevent infinite correction loops.

8.  **Execution Approval**

    -   Only queries that pass all validation checks are submitted to Spark for execution.

------------------------------------------------------------------------

## Key Features

### Natural Language to SQL

Users can query business datasets using plain English. The AI engine converts the question into Spark SQL using schema and metric context.

**Example**

User question:

> Top 5 regions by revenue in 2023

Generated SQL:

``` sql
SELECT region,
       SUM(total_revenue) AS revenue
FROM revenue_by_region
WHERE year = 2023
GROUP BY region
ORDER BY revenue DESC
LIMIT 5;
```

------------------------------------------------------------------------

### Semantic Data Layer

The platform uses a **semantic layer** to provide context to the AI model.

    semantic_layer/
     ├── schema_context.json
     └── metrics_catalog.json

This ensures the AI only uses:

-   approved tables
-   approved columns
-   defined metrics

This reduces hallucinated SQL and improves reliability.

------------------------------------------------------------------------

### SQL Governance & Validation

All AI-generated SQL queries pass through a **validation layer** before execution.

The validator checks for:

-   destructive SQL operations (`DROP`, `DELETE`, etc.)
-   wildcard queries (`SELECT *`)
-   multiple statements
-   unbalanced SQL syntax
-   invalid AI responses

This prevents unsafe or expensive queries from running in the Spark environment.

------------------------------------------------------------------------

### Query Cost Protection

To prevent expensive or inefficient queries from overloading the compute engine, the platform implements **query cost protection guardrails**.

These safeguards ensure that AI-generated queries remain efficient and suitable for interactive analytics workloads.

The validation layer enforces several rules before a query can execute:

-   **Maximum JOIN limit** -- prevents excessive joins that can trigger large distributed shuffles.
-   **Mandatory result limit** -- all queries must include a `LIMIT` clause.
-   **Maximum result size (`LIMIT ≤ 100`)** -- prevents extremely large result sets.
-   **Filter requirement (`WHERE` clause)** -- helps avoid full table scans.

------------------------------------------------------------------------

### Query Logging

All executed queries are automatically logged for **observability, auditing, and debugging**.

Each query execution generates a structured log entry containing:

-   unique query identifier (`query_id`)
-   timestamp
-   original user question
-   generated SQL query
-   execution status
-   query execution time
-   validation error message (if any)

Logs are stored as **daily log files** to keep log sizes manageable and simplify monitoring.

------------------------------------------------------------------------

### Distributed Query Execution

Queries are executed using **Apache Spark**, enabling scalable analytics across large datasets.

Gold-layer datasets are automatically registered as Spark temporary views:

    data/gold/
     ├── revenue_by_region
     ├── revenue_by_channel
     └── customer_summary

The system dynamically loads these datasets into Spark for query execution.

------------------------------------------------------------------------

## Example Workflow

1.  User submits a business question

> Select top 5 rows by revenue in the EMEA region for the year 2023

2.  AI generates SQL
3.  SQL passes validation checks
4.  Query executes in Spark
5.  Results are returned to the user

------------------------------------------------------------------------

## Running the Project

### 1. Install dependencies

``` bash
pip install pyspark openai
```

### 2. Configure environment variables

``` bash
export API_KEY="your_api_key"
export BASE_URL="your_llm_endpoint"
export MODEL="model_name"
```

### 3. Run the application

``` bash
spark-submit main.py
```

------------------------------------------------------------------------

## Author

**Thushan Withanage**

Last Updated: 13th March 2026