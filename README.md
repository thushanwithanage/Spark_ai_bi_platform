# Governed AI Self-Service BI Platform

An AI-powered Business Intelligence platform that enables users to query curated business datasets using natural language. The system converts user questions into validated Spark SQL queries and executes them securely against governed analytics datasets.

This project demonstrates the way modern analytics platforms combine **AI, data governance, RBAC, and distributed data processing** to enable safe and scalable self-service analytics.

---

## Project Overview

Traditional BI workflows require business users to depend on data teams to write SQL queries or build dashboards.

This platform enables users to ask questions such as:

> **Top 5 revenue generating regions in 2023**

The system automatically:

1. Converts the question into SQL using an LLM
2. Validates the generated SQL for safety, governance, and RBAC permissions
3. Optimizes the SQL query for performance and efficiency
4. Executes the query in Spark
5. Returns the results to the user

The architecture ensures that AI-generated queries remain **controlled, secure, optimized, and aligned with the data model and user permissions**.

---

## Architecture

```
User Question
      │
      ▼
Natural Language → SQL Engine
      │
      ▼
SQL Validation & Optimization Layer
      │
      ├── Valid → Execute in Spark with RBAC enforcement
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
```

---

### Key Design Principles

* Semantic layer driven analytics
* AI-assisted query generation
* Governed SQL validation
* SQL query optimization
* Role-Based Access Control (RBAC) enforcement
* Distributed query execution using Spark

---

## AI Query Validation & Optimization Pipeline

AI-generated SQL queries pass through a multi-stage validation and optimization pipeline before execution. This architecture ensures that natural language queries remain **safe, governed, RBAC-compliant, optimized, and cost-efficient**.

The pipeline performs the following validation and optimization steps:

1. **SQL Extraction**

   * Extracts valid SQL statements from the LLM response.
   * Removes markdown formatting or explanatory text.

2. **Syntax Parsing**

   * SQL queries are parsed using `sqlglot` to ensure valid SQL syntax.
   * Prevents execution of malformed queries.

3. **Semantic Layer Validation**

   * Ensures the query only references **approved tables and columns** defined in the semantic layer.

4. **Security Validation**

   * Blocks destructive SQL operations such as `DROP`, `DELETE`, `UPDATE`.

5. **RBAC Enforcement**

   * Verifies that the user is authorized to access the requested tables and columns.
   * Ensures sensitive datasets are protected according to organizational roles.

6. **Query Cost Protection**

   * Enforces safeguards to prevent expensive queries: maximum JOIN count, mandatory `LIMIT`, maximum result size (`LIMIT ≤ 100`), `WHERE` filter requirement.

7. **Query Optimization Layer**

   * Applies rule-based SQL optimizations using `sqlglot` to improve query performance before execution.
   * Optimizations include:
     * **Predicate Pushdown** – Moves filters closer to data sources to reduce data scanned.
     * **Subquery Detection** – Identifies presence of nested queries.
     * **Unused Column Pruning in Subqueries** – Removes unnecessary columns from subqueries to reduce data processing overhead.
     * **Subquery Rewriting** – Updates and merges optimized subqueries back into the main query.

   These optimizations ensure queries are **efficient, scalable, and cost-effective** when executed in Spark.

8. **Query Logging**

   * Logs each query execution with a unique `query_id`, timestamp, user question, generated SQL, validation status, execution time, and RBAC check result.

9. **AI Query Correction Loop**

   * Failed validations due to syntax errors, missing clauses, invalid tables are sent back to the AI for correction.
   * Re-validated before execution with a retry limit.

10. **Execution Approval**

   * Only queries passing all validations, optimizations, and RBAC checks are executed in Spark.

---

## Key Features

### Natural Language to SQL

Users can query business datasets using plain English. The AI engine converts the question into Spark SQL using schema, metric context, and user permissions.

### Semantic Data Layer

The platform uses a **semantic layer** to provide context and restrict access.

```
semantic_layer/
 ├── schema_context.json
 └── metrics_catalog.json
```

Ensures the AI only uses approved tables, columns, and defined metrics.

### SQL Governance, RBAC & Validation

All AI-generated SQL queries pass through a **validation and optimization layer** that enforces:

* Destructive SQL checks (`DROP`, `DELETE`, etc.)
* Wildcard and multi-statement checks
* RBAC enforcement
* Query cost protection
* Query optimization
* Logging and auditing

### SQL Query Optimization Engine

The platform includes a rule-based SQL optimization engine powered by `sqlglot`, enabling automatic improvements to AI-generated queries before execution.

Key capabilities:

* Predicate pushdown for reducing data scans
* Detection of subqueries in complex SQL
* Removal of unused columns from subqueries
* Safe rewriting and merging of optimized subqueries

This ensures that generated queries are not only valid and secure, but also **performance optimized for distributed execution in Spark**.

### Distributed Query Execution

Queries are executed using **Apache Spark**, enabling scalable analytics across large datasets. Gold layer datasets are automatically registered as Spark temporary views.

### Example Workflow

1. User submits a business question
2. AI generates SQL
3. SQL passes validation, optimization, and RBAC checks
4. Query executes in Spark
5. Results are returned to the user

---

## Running the Project

### 1. Install dependencies

```bash
pip install pyspark openai
```

### 2. Configure environment variables

```bash
export API_KEY="your_api_key"
export BASE_URL="your_llm_endpoint"
export MODEL="model_name"
```

### 3. Run the application

```bash
spark-submit main.py
```

---

## Author

**Thushan Withanage**

Last Updated: 31st March 2026