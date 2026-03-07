# Governed AI Self-Service BI Platform

An AI-powered self-service Business Intelligence platform that enables users to query curated business datasets using natural language. The system converts user questions into validated Spark SQL queries and executes them securely against governed analytics datasets.

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
          ▼
    Spark Query Execution
          │
          ▼
    Results Returned to User

### Key Design Principles

-   Semantic layer driven analytics
-   AI-assisted query generation
-   Governed SQL validation
-   Distributed query execution using Spark

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

### Distributed Query Execution

Queries are executed using **Apache Spark**, enabling scalable analytics across large datasets.

Gold-layer datasets are automatically registered as Spark temporary views:

    data/gold/
     ├── revenue_by_region
     ├── revenue_by_channel
     └── customer_summary

The system dynamically loads these datasets into Spark for query execution.

------------------------------------------------------------------------

## Technology Stack

  Component         Technology
  ----------------- -----------------------------
  Data Processing   Apache Spark
  Programming       Python
  Query Language    SQL
  AI Engine         LLM (OpenAI-compatible API)
  Data Storage      Parquet
  Architecture      Medallion Data Model

------------------------------------------------------------------------

## Example Workflow

1.  User submits a business question

```{=html}
<!-- -->
```
    Select top 5 rows by revenue in the EMEA region for the year 2023

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

**Thushan Withanage**\

Last Updated: 7th March 2026