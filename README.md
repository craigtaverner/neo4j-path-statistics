# Example procedures for calculating path statistics

When Cypher queries are planned, one key factor that influences planning are the graph statistics of the actual database being run. When the cost planner was first introduced in Neo4j 2.2, the graph statistics available included only a few things like:

* Number of nodes with specific labels like (:Label)
* Number of relationships with specific types like ()-[:TYPE]->()
* Number of length-1 patterns, where only one label is known like:
  * ()-[:TYPE]->(:Label)
  * (:Label)-[:TYPE]->()

However no statistics are kept for anything more complex.

The procedures provided here are designed for investigative use in determining the statistics of longer pattern expressions.

## Testing in Intellij

Import the project as a maven project. Find the PathStatisticsProceduresTest class and run all tests.

## Installing and testing in Neo4j

On the command-line run:

    mvn clean package
    cp target neo4j-path-statistics-1.0-SNAPSHOT.jar PATH_TO_NEO4J_31_INSTALLATION/plugins/
    PATH_TO_NEO4J_31_INSTALLATION/bin/neo4j restart

Then open the browser at http://localhost:7474 and run the following command:

    CALL path.stats(5)

This should list five rows, one for each path length, and the second column will be the number of paths found with that length. Leaving out the maxDepth field will default to 5.

For more information on the syntax of the command run `CALL dbms.procedures` to see all procedures and their signatures. To see only the path procedures use:

    CALL dbms.procedures() YIELD name, signature, description WHERE name STARTS WITH 'path' RETURN *

## Can this be done in Cypher?

The simple `path.stats(5)` call above can be done in Cypher:

    MATCH p=(a)-[*0..5]->(b) RETURN length(p) as length, count(p) as value

_Note: The current implementation of the procedure does not handle self relationships, and so if those exist, then the procedure will give different results to Cypher._
