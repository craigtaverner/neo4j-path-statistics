package org.amanzi.neo4j.paths;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.procedure.*;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.logging.Log;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * Sample procedures, provided here as a template. Also used in test code.
 */
public class PathStatisticsProcedures {
    @Context
    public GraphDatabaseService graph;

    @Description("Produce a table of path lengths and number of paths found for each length")
    @Procedure( "path.stats" )
    public Stream<LengthResult> getStats( @Name( value = "maxDepth", defaultValue = "5") long maxDepth ) throws ProcedureException
    {
        LinkedHashMap<Integer,Long> results = new LinkedHashMap<>();
        graph.getAllNodes().stream().forEach( node ->
        {
            TraversalDescription td =
                    graph.traversalDescription().depthFirst().evaluator( Evaluators.toDepth( (int)maxDepth ) );
            for ( Path path : td.traverse( node ) )
            {
                int length = path.length();
                Long count = results.get( length );
                results.put( length, count == null ? 1 : count + 1 );
            }
        } );
        return IntStream.range( 0, (int)maxDepth ).mapToObj(
                depth -> new LengthResult( depth, results.containsKey( depth ) ? results.get( depth ) : 0 ) );
    }

    @Description("Produce a table of label paths and number of paths found for each length")
    @Procedure("labelpath.stats")
    public Stream<LabelResult> getLabelPathStats(@Name(value = "maxDepth", defaultValue = "3") long maxDepth) throws ProcedureException {
        LinkedHashMap<String, Long> results = new LinkedHashMap<>();
        LogService logService = ((GraphDatabaseAPI) graph).getDependencyResolver().resolveDependency(LogService.class);
        Log log = logService.getUserLog(getClass());
        graph.getAllNodes().stream().forEach(node ->
        {
            TraversalDescription td =
                    graph.traversalDescription().depthFirst().evaluator(Evaluators.toDepth((int) maxDepth));
            for (Path path : td.traverse(node)) {
                StringBuilder sb = new StringBuilder();
                Iterator<Relationship> iter = path.relationships().iterator();
                while (iter.hasNext()) {
                    Relationship rs = iter.next();
                    sb.append(rs.getProperty("label")).append(",");
                }

                int length = path.length();
                if (length > 0) {
                    sb.setLength(sb.length() - 1);
                }
                String pathLabel = sb.toString();
                Long count = results.get(pathLabel);

                results.put(pathLabel, count == null ? 1 : count + 1);
            }
        });
        return results.entrySet().stream().map(e -> new LabelResult(e.getKey(), e.getValue()));
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
        public String label;
        public long value;

        public LabelResult(String label, long value) {
            this.label = label;
            this.value = value;
        }
    }
}
