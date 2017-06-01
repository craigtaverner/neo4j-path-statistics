package org.amanzi.neo4j.paths;

import javafx.beans.binding.StringBinding;
import org.neo4j.cypher.internal.frontend.v3_1.ast.functions.Relationships;
import org.neo4j.cypher.internal.frontend.v3_1.symbols.RelationshipType;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.procedure.*;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.*;

import org.neo4j.logging.Log;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import tue.we.li.histogram.Histogram;
import tue.we.li.Util;
import tue.we.li.histogram.*;

import static java.util.stream.Collectors.toList;
import static org.neo4j.graphdb.Direction.*;

/**
 * Sample procedures, provided here as a template. Also used in test code.
 */
public class PathStatisticsProcedures {
    final static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Context
    public GraphDatabaseService graph;

    @Description("Produce a table of path lengths and number of paths found for each length")
    @Procedure("path.stats")
    public Stream<LengthResult> getStats(@Name(value = "maxDepth", defaultValue = "5") long maxDepth) throws ProcedureException {
        LinkedHashMap<Integer, Long> results = new LinkedHashMap<>();
        graph.getAllNodes().stream().forEach(node ->
        {
            TraversalDescription td =
                    graph.traversalDescription().depthFirst().evaluator(Evaluators.toDepth((int) maxDepth));
            for (Path path : td.traverse(node)) {
                int length = path.length();
                Long count = results.get(length);
                results.put(length, count == null ? 1 : count + 1);
            }
        });
        return IntStream.range(0, (int) maxDepth).mapToObj(
                depth -> new LengthResult(depth, results.containsKey(depth) ? results.get(depth) : 0));
    }

    @Description("Produce a table of label paths and number of paths found for each unique label path")
    @Procedure("labelpath.sele")
    public Stream<LabelResult> getLabelPathSelectivies(@Name(value = "maxDepth", defaultValue = "3") long maxDepth,
                                                       @Name(value = "method", defaultValue = "D") String method) throws ProcedureException {

        LinkedHashMap<String, Long> results = new LinkedHashMap<>();

        List<String> labels = graph.getAllRelationshipTypes().stream().map(x -> x.name()).collect(toList());
        Collections.sort(labels);
        for (String label : labels) {
            logger.debug(label);
        }
        //k max length
        int base = labels.size();
        logger.info("size of unique label: " + base);
        int k = (int) maxDepth;
        int totalPathNumber = Util.getListSize(k, base);

        logger.info("total number of label paths: " + totalPathNumber);
        List<Long> selectivities = new ArrayList<Long>(Collections.nCopies(totalPathNumber, 0l));
        Uniqueness uni = Uniqueness.NONE;

        graph.getAllNodes().stream().forEach(node ->
        {
            TraversalDescription td = null;
            switch (method) {
                case "D":
                    td = graph.traversalDescription()
                            .expand(PathExpanderBuilder.allTypes(Direction.OUTGOING).build())
//                            .relationships(null, Direction.OUTGOING)
                            .depthFirst().uniqueness(uni)
                            .evaluator(Evaluators.toDepth((int) maxDepth));
                    break;
                case "B":
                    td = graph.traversalDescription()
                            .expand(PathExpanderBuilder.allTypes(Direction.OUTGOING).build())
//                            .relationships(null, Direction.OUTGOING)
                            .breadthFirst().uniqueness(uni)
                            .evaluator(Evaluators.toDepth((int) maxDepth));
                    break;
                default:
                    break;
            }
            for (Path path : td.traverse(node)) {
                StringBuilder sb = new StringBuilder();
                Iterator<Relationship> iter = path.relationships().iterator();
                List<Integer> pathIdentifier = new ArrayList<>();
                while (iter.hasNext()) {
                    Relationship rs = iter.next();
                    String label = rs.getType().name();
                    int labelIndex = labels.indexOf(label);
                    pathIdentifier.add(labelIndex);
                    sb.append(label);
                    sb.append(LabelResult.split);
                }

                int length = path.length();
                if (length > 0) {
                    sb.setLength(sb.length() - LabelResult.split.length());
                }
                String pathLabel = sb.toString();

                int pathIndex = Util.list2Int(pathIdentifier, base);
                if (pathIndex == -1) {
//                    logger.debug(Util.list2LabelPath(pathIdentifier));
                } else {
                    try {
                        selectivities.set(pathIndex, selectivities.get(pathIndex) + 1);
                    } catch (Exception e) {
                        logger.error(pathIndex + "");
                    }
                }
                Long count = results.get(pathLabel);
                results.put(pathLabel, count == null ? 1 : count + 1);
            }
        });
        logger.info("travel ends");
        return IntStream.range(0, selectivities.size())
                .mapToObj(index ->
                        new LabelResult(Util.list2LabelPath(
                                Util.int2List(index, base).stream().map(x -> labels.get(x)).collect(toList())
                        ), selectivities.get(index)));
    }

    public static class LengthResult {
        public long length;
        public long value;

        public LengthResult(long length, long value) {
            this.length = length;
            this.value = value;
        }
    }

    public static class LabelResult {
        public final static String split = ">";

        public String label;
        public long value;

        public LabelResult(String label, long value) {
            this.label = label;
            this.value = value;
        }
    }
}
