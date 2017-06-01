package org.amanzi.neo4j.paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.Map;

import static org.amanzi.neo4j.paths.TestUtil.testCallCount;
import static org.amanzi.neo4j.paths.TestUtil.testResult;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class PathStatisticsProceduresTest {

    private GraphDatabaseService db;


    private String graphDbDir = "/home/liwang/neo4j/neo4j-community-3.1.1/data/databases";
    private String pathToConfig = "/home/liwang/neo4j/neo4j-community-3.1.1/conf/";

    @Before
    public void setUp() throws Exception {
//        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(graphDbDir + "/health"));
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class)
                .registerProcedure(PathStatisticsProcedures.class);
//        try (Transaction tx = db.beginTx()) {
//            Label label = Label.label("User");
//            RelationshipType relType = RelationshipType.withName("KNOWS");
//            Node root = db.createNode(label);
//            for (int depth = 1; depth < 10; depth++) {
//                Node prev = root;
//                for (int i = 0; i < depth; i++) {
//                    Node child = db.createNode(label);
//                    prev.createRelationshipTo(child, relType);
//                    prev = child;
//                }
//            }
//            tx.success();
//        }
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testCountPathsDepthSix() {
        long maxDepth = 6;
        testCallCount(db, "CALL path.stats(" + maxDepth + ")", null, (int) maxDepth);
        testResult(db, "CALL path.stats(" + maxDepth + ")", result ->
        {
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                System.out.println(row);
                assert (row.containsKey("length"));
                assert (row.containsKey("value"));
                long length = (Long) row.get("length");
                long count = (Long) row.get("value");
                assertThat(length, greaterThanOrEqualTo(0L));
                assertThat(length, lessThan(maxDepth));
                assertThat(count, greaterThan(0L));
            }
        });
    }

    @Test
    public void testCountPathsDefaultDepth() {
        testCallCount(db, "CALL path.stats", null, 5);
    }
}
