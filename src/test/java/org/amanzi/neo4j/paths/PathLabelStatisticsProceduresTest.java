package org.amanzi.neo4j.paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.function.Consumer;

import static org.amanzi.neo4j.paths.TestUtil.testCallCount;
import static org.amanzi.neo4j.paths.TestUtil.testResult;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static tue.we.li.Main.BASE_FOLDER;

public class PathLabelStatisticsProceduresTest {
    final static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private GraphDatabaseService db;

    private String graphDbDir = "/home/liwang/neo4j/neo4j-community-3.1.1/data/databases/";
    private String pathToConfig = "/home/liwang/neo4j/neo4j-community-3.1.1/conf/";

    //    private String dataset = "libimseti";
    private String dataset = "moreno_health";

    @Before
    public void setUp() throws Exception {
        db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(graphDbDir + dataset));
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class)
                .registerProcedure(PathStatisticsProcedures.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testCountPathsDepthTwo() {
        long maxDepth = 6;
        String method = "B";
        long t0 = System.currentTimeMillis();
        testResult(db, "CALL labelpath.sele(" + maxDepth + ",\"" + method + "\")", new ResultConsumer(dataset, maxDepth, method));
        long t1 = System.currentTimeMillis();
        logger.info(method + " runs for " + (t1 - t0));
        method = "D";
        testResult(db, "CALL labelpath.sele(" + maxDepth + ",\"" + method + "\")", new ResultConsumer(dataset, maxDepth, method));
        long t2 = System.currentTimeMillis();
        logger.info(method + " runs for " + (t2 - t1));
    }
}

class ResultConsumer implements Consumer<org.neo4j.graphdb.Result> {
    public ResultConsumer(String dataset, long maxDepth, String method) {
        this.dataset = dataset;
        this.maxDepth = maxDepth;
        this.method = method;
    }

    private String dataset;
    private long maxDepth;
    private String method;

    @Override
    public void accept(org.neo4j.graphdb.Result result) {
        StringBuilder sb = new StringBuilder("label,value");
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            assert (row.containsKey("label"));
            assert (row.containsKey("value"));
            String label = (String) row.get("label");
            long count = (Long) row.get("value");
            sb.append("\n");
            sb.append(label).append(",").append(count);
        }
        try (PrintWriter out = new PrintWriter(method + maxDepth + "_neo.csv")) {
            out.println(sb.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
