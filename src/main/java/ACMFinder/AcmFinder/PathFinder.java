package ACMFinder.AcmFinder;

import java.io.File;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import ACMFinder.constants.Constants;

public class PathFinder {


	public void getPathBetweenNodes(String authorId, String paperACMId)
	{

		File databaseDirectory = new File(Constants.NEO_GRAPH_DB_PATH);
		GraphDatabaseService dbService =  new GraphDatabaseFactory().newEmbeddedDatabase(databaseDirectory);
		try (Transaction tx=dbService.beginTx()) 
		{
			Node authorNode = dbService.findNode(Label.label("author"), "id", authorId);
			Node paperNode = dbService.findNode(Label.label("paper"), "acmID", paperACMId);

			System.out.println("\nAuthor: " + authorNode);
			System.out.println("Paper: " + paperNode);
			
			PathExpander<Object> pathExpander = PathExpanders.allTypesAndDirections();
			CostEvaluator<Double> costEvaluator = new RelationshipWeightEvaluator();
			EstimateEvaluator<Double> heuristicEstimator = new HeuristicEstimator();
			
			org.neo4j.graphalgo.PathFinder<WeightedPath> keyPathFinder = GraphAlgoFactory.aStar(pathExpander, costEvaluator, heuristicEstimator);
		
			Path path = keyPathFinder.findSinglePath(authorNode, paperNode);
			
			
			
			System.out.println("\nKey Path:"+ path);

			tx.success();
		}
		dbService.shutdown();
	}
}

class HeuristicEstimator implements EstimateEvaluator<Double>{

	@Override
	public Double getCost(Node source, Node destination) {
		
		PathExpander<Object> pathExpander = PathExpanders.allTypesAndDirections();
		
		org.neo4j.graphalgo.PathFinder<Path> shortestPathFinder = GraphAlgoFactory.shortestPath(pathExpander, 9);
	
		Path path = shortestPathFinder.findSinglePath(source, destination);
		
		Double cost = 0.0;
		for(org.neo4j.graphdb.Relationship relationship: path.relationships())
		{
			cost += (Double)relationship.getProperty("weight");
		}
		
		return (cost);
	}
}

class RelationshipWeightEvaluator implements CostEvaluator<Double>{

	@Override
	public Double getCost(org.neo4j.graphdb.Relationship realtionship, Direction arg1) {
		// TODO Auto-generated method stub
		Double cost = (Double)realtionship.getProperty("weight");
		
		return (cost);
	}
	
}

