# Governed AI Self-Service BI Platform

## Overview
An end-to-end LLM-based Business Intelligence platform that enables users to query curated datasets using natural language.

---

## Key Achievements

- Built an LLM-based analytics platform converting natural language to SQL queries
- Designed and implemented a **SQL validation and governance layer**
- Developed a **query optimization engine using sqlglot**
- Implemented **Role-Based Access Control (RBAC)** for secure data access
- Integrated **Apache Spark** for scalable query execution
- Built an interactive **Streamlit UI** for real-time analytics
- Implemented **automatic query correction loop**
- Reduced query inefficiencies via **predicate pushdown and join pruning**

---

## Architecture Diagram

```
                ┌──────────────────────────┐
                │        User (UI)         │
                │     Streamlit App        │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │  Natural Language Input  │
                └────────────┬─────────────┘
                             ▼
                ┌──────────────────────────┐
                │   AI NL → SQL Engine     │
                └────────────┬─────────────┘
                             ▼
                ┌──────────────────────────┐
                │   SQL Validation Layer   │
                │ - Syntax Check           │
                │ - Security Validation    │
                │ - Semantic Validation    │
                └────────────┬─────────────┘
                             ▼
                ┌──────────────────────────┐
                │   Optimization Engine    │
                │ - Predicate Pushdown     │
                │ - Subquery Optimization  │
                │ - Unused Join Removal    │
                └────────────┬─────────────┘
                             ▼
                ┌──────────────────────────┐
                │        RBAC Layer        │
                │  Role-based Filtering    │
                └────────────┬─────────────┘
                             ▼
                ┌──────────────────────────┐
                │     Spark Execution      │
                │   (Distributed Engine)   │
                └────────────┬─────────────┘
                             ▼
                ┌──────────────────────────┐
                │        Results UI        │
                └──────────────────────────┘
```

---

## Core Components

### 1. AI Query Generation
- Converts user questions into SQL using LLMs
- Uses schema-aware prompts for accuracy

---

### 2. SQL Validation & Governance
- SQL parsing using `sqlglot`
- Blocks unsafe operations (DROP, DELETE, UPDATE)
- Ensures schema compliance

---

### 3. Query Optimization Engine

Key optimizations:

- Predicate Pushdown
- Subquery Column Pruning
- Subquery Rewriting
- Unused Join Removal

---

### 4. RBAC (Role-Based Access Control)

- Restricts access to datasets and columns
- Ensures secure data usage based on roles

---

### 5. Execution Layer

- Uses Apache Spark for scalable execution
- Loads Parquet datasets as temporary views

---

### 6. Streamlit UI

- Accepts natural language queries
- Displays generated SQL
- Shows results and execution time
- Handles errors and retries

---

### 7. AI Correction Loop

- Detects invalid queries
- Sends feedback to LLM
- Automatically retries corrected SQL

---

## Tech Stack

- Python
- Apache Spark (PySpark)
- Streamlit
- SQLGlot
- Parquet Data Format

---

## ▶️ How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the application
```bash
streamlit run app.py
```

---

## Example Query

**Input:**
```
Top 5 revenue generating regions
```

**Output:**
- Generated SQL
- Optimized query
- Execution results in table format

---

## 👤 Author
Thushan Withanage