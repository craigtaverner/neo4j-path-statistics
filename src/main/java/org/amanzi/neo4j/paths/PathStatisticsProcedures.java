package org.amanzi.neo4j.paths;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.procedure.*;

import java.util.LinkedHashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Sample procedures, provided here as a template. Also used in test code.
 */
public class PathStatisticsProcedures
{
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

    public static class LengthResult
    {
        public long length;
        public long value;

        public LengthResult( long length, long value )
        {
            this.length = length;
            this.value = value;
        }
    }
}
