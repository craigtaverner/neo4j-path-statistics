# Example procedures for calculating path statistics

When Cypher queries are planned, one key factor that influences planning are the graph statistics of the actual database being run. When the cost planner was first introduced in Neo4j 2.2, the graph statistics available included only a few things like:

* Number of nodes with specific labels like (:Label)
* Number of relationships with specific types like ()-[:TYPE]->()
* Number of length-1 patterns, where only one label is known like:
  * ()-[:TYPE]->(:Label)
  * (:Label)-[:TYPE]->()

However no statistics are kept for anything more complex.

The procedures provided here are designed for investigative use in determining the statistics of longer pattern expressions.
