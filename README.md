# DebCypher: A Structural Debugger for Cypher Queries

**DebCypher** is an automated debugging framework for the Cypher graph query language. It addresses the "Empty Answer" problem (Why-Not provenance) by programmatically deconstructing complex declarative patterns into a sequence of testable sub-queries to isolate the exact point of failure.

This implementation is the practical companion to the research paper: *"Design and Implementation of a Specialized Debugger for the Cypher Graph Database Query Language."*

---

## 🚀 The Core Concept: MNES

When a Cypher query returns no results, developers often struggle to distinguish between data-level inconsistencies and logic errors. DebCypher identifies the **Maximal Non-Empty Sub-queries (MNES)**—the largest possible parts of your query that still return results. By finding the "frontier of success," the debugger can point to the specific relationship or predicate that caused the query to "break."



## 🛠 Features

-   **AST Decomposition:** Leverages the **Cypher-DSL** framework to break down queries into monotonic, increasing sequences.
-   **Multi-Frontier Analysis:** Initiates parallel debugging traces from every node anchor defined in the query to find disjoint successful branches.
-   **Multi-Layer Validation:**
    -   **Schema Conformance:** Verifies Node Labels, Relationship Types, and Property Keys.
    -   **Boolean Satisfiability:** Checks the logic of `WHERE` clauses for type mismatches.
    -   **Structural Connectivity:** Isolates missing edges or paths in the graph instance.
-   **Levenshtein Recommendations:** Suggests corrections for typos in labels or properties using edit-distance logic.

## 📋 How it Works

The debugger applies a **Decomposition Operator** ($\omega$) to the query. For a complex join that fails, the tool generates a trace:

1.  **Seed:** `MATCH (p:Person {name:'Tom Hanks'})` ✅
2.  **Expansion:** `MATCH (p)-[:ACTED_IN]->(m:Movie)` ✅
3.  **Parallel Seed:** `MATCH (d:Person {name:'Nolan'})` ✅
4.  **Join Attempt:** `MATCH (p)-[:ACTED_IN]->(m)<-[:DIRECTED]-(d)` ❌

**Diagnosis:** The debugger identifies that both the "Hanks" and "Nolan" branches are valid, but their **intersection** is empty.



## 🔧 Installation

### Requirements
-   **Java 17+**
-   **Maven 3.8+**
-   **Neo4j Instance** (The debugger requires a connection to probe the data)

### Build
```bash
git clone [https://github.com/jalmenUAL/debcypher.git](https://github.com/jalmenUAL/debcypher.git)
cd debcypher
mvn clean install

