# Chameleon Tasks

## Sample Queries

 - `EXPLAIN SELECT * FROM taxi_trajectory WHERE properly_contained(polyline, 'POLYGON((41.15875373498798 -8.62638544999368, 41.19441480447213 -8.62638544999368, 41.19441480447213 -8.54498526523022, 41.15875373498798 -8.54498526523022))') LIMIT 50`

## Jun 17, 2025

### How to cache the queries?

#### Goal

- Cache the queries. But how many types of queries do we have?
  - `SELECT * FROM table` Every other query is a subset of this query.
  - `SELECT * FROM table WHERE predicate_1 AND predicate_2`
    - 
    - `SELECT col_1, col_2 FROM table WHERE predicate_1 AND predicate_2` It is a subset of the previous query, so result should be returned from the same cache.
  - `SELECT * FROM table WHERE predicate_1 AND predicate_2 AND contained(col_polyline, 'strict/relaxed', (x1, y1, x2, y2, x3, y3, x4, y4))`
    - `SELECT * FROM table WHERE predicate_1 AND predicate_2 AND contained(col_polyline, 'strict/relaxed', (xx1, yy1, xx2, yy2, xx3, yy3, xx4, yy4))` It is a subset of the previous query if the bounding box is the same or smaller. So, the result should be returned from the same cache.
  - `SELECT * FROM table WHERE predicate_1 AND predicate_2 LIMIT n1`
    - `SELECT * FROM table WHERE predicate_1 AND predicate_2 LIMIT n2` It is a subset of the previous query if n2 <= n1. So, the result should be returned from the same cache.
    - `SELECT * FROM table WHERE predicate_1 AND predicate_2 LIMIT n3 OFFSET m` It is a subset of the previous query if n3 + m <= n1. So, the result should be returned from the same cache.


### How to map the road data in case of proto taxi?

Info
 - https://fmm-wiki.github.io/

### Core Idea

1. Add a custom operator to datafusion using `UserDefinedLogicalNodeCore`.
2. Add a custom query optimizer rule using `OptimizerRule`.
3. This query optimizer checks if the query is a subset of a cached query or not. If it is, it transforms the logical plan to use the custom operator.
4. Implement the `ExtensionPlanner` for the custom operator to handle the execution of the cache query. Here, we will return the `ExecutionPlan` of the cached query, which will then return the `RecordBatches` from the cache.
5. At what stage do we cache the query data results if it is not already cached?

