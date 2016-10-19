package graphminer;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import constant.Constants;
import data.Author;
import data.KeyTopicPath;
import data.Paper;

public class KeyNodeFinder {

	public static int currentThreadCount;
	public static final int  maxThreadCount = Constants.MAXIMUM_THREAD_COUNT;

	public void findKeyNodesForAuthorAndConference(Author author, List<Paper> conferencePapers)
	{
		assert author !=null : "Invalid author";
		assert conferencePapers != null & !conferencePapers.isEmpty() : "Invalid list of conference papers.";

		List<String> queryPaperIds = author.writtenPaperACMIds;

		currentThreadCount = 0;
		PathFinderHelper pathFinderHelper;
		Iterator<Paper> paperIterator = conferencePapers.iterator();
		while(paperIterator.hasNext())
		{
			try
			{
				if(currentThreadCount < maxThreadCount)
				{
					Paper conferencePaper= paperIterator.next();

					String targetPaperID = conferencePaper.acmID;
					pathFinderHelper = new PathFinderHelper();
					pathFinderHelper.setData(queryPaperIds, targetPaperID, author, conferencePaper);
					pathFinderHelper.start();

					threadStarted();

				}
				else
				{
					Thread.sleep(2000);
				}


			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}

		while(currentThreadCount > 0)
		{
			try {
				Thread.sleep(4000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		PathFinderHelper.cleanup();

		if(PathFinderHelper.targetPaperIdtoPathRWProbability != null & 
				PathFinderHelper.targetPaperIdtoPathRWProbability.isEmpty() == false)
		{
			Map<String, Double> targetPaperIdToPathRWProbability = new HashMap<String,Double>(PathFinderHelper.targetPaperIdtoPathRWProbability);
			PathFinderHelper.targetPaperIdtoPathRWProbability.clear();

			if(targetPaperIdToPathRWProbability.size() > 11)
			{
				author.paperIDToRWProability = this.getTop10Entries(targetPaperIdToPathRWProbability);
			}
			else
			{
				author.paperIDToRWProability = targetPaperIdToPathRWProbability;
			}
		}
		//KeyTopicPathFinder.dbService.shutdown();
	}

	
	public <String, Double extends Comparable<? super Double>> Map<String, Double> getTop10Entries(Map<String, Double> map) {
		return map.entrySet()
				.stream()
				.sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
				.limit(10)
				.collect(Collectors.toMap(
						Map.Entry::getKey, 
						Map.Entry::getValue, 
						(e1, e2) -> e1, 
						LinkedHashMap::new
						));
	}

	public static synchronized void threadCompleted()
	{
		currentThreadCount--;
	}

	public static synchronized void threadStarted()
	{
		currentThreadCount++;
	}

}

class ACMNodeFilter implements Predicate<Node>{

	@Override
	public boolean test(Node testNode) {
		// TODO Auto-generated method stub
		boolean isNodeMatch = false;

		if(testNode.hasLabel(Label.label("paper")) | testNode.hasLabel(Label.label("topic")))
		{
			isNodeMatch = true;
		}

		return isNodeMatch;
	}

}

class ACMRelationshipFilter implements Predicate<Relationship>{

	@Override
	public boolean test(Relationship relationship) {
		// TODO Auto-generated method stub
		boolean isNodeMatch = true;

		if(relationship.isType(RelationshipType.withName("co_author")) | 
				relationship.isType(RelationshipType.withName("publishedat")))
		{
			isNodeMatch = false;
		}

		return isNodeMatch;
	}

}

class PathFinderHelper implements Runnable{

	public List<String> queryPaperIDs;
	public String targetPaperID;
	public Author author = null;
	public Paper paper = null;
	private Thread t = null;
	public static File databaseDirectory = null;
	public static GraphDatabaseService dbService =null;
	public static Map<String, Double> targetPaperIdtoPathRWProbability = new ConcurrentHashMap<String, Double>();
	private PathExpander<Object> pathExpander = null;

	public PathFinderHelper() {
		if(pathExpander == null)
		{
			PathExpanderBuilder pathExpanderBuilder = PathExpanderBuilder.empty();

			//			ACMRelationshipFilter relationshipFilter = new ACMRelationshipFilter();
			//			pathExpanderBuilder = pathExpanderBuilder.addRelationshipFilter(relationshipFilter);
			//			pathExpanderBuilder = pathExpanderBuilder.addRelationshipFilter(relationshipFilter);
			//			pathExpanderBuilder = pathExpanderBuilder.addRelationshipFilter(relationshipFilter);
			//			pathExpanderBuilder = pathExpanderBuilder.addRelationshipFilter(relationshipFilter);
			//			pathExpanderBuilder = pathExpanderBuilder.addRelationshipFilter(relationshipFilter);
			//			pathExpanderBuilder = pathExpanderBuilder.addRelationshipFilter(relationshipFilter);


			ACMNodeFilter nodeFilter = new ACMNodeFilter();
			pathExpanderBuilder = pathExpanderBuilder.addNodeFilter(nodeFilter);
			pathExpanderBuilder = pathExpanderBuilder.addNodeFilter(nodeFilter);
			pathExpanderBuilder = pathExpanderBuilder.addNodeFilter(nodeFilter);
			pathExpanderBuilder = pathExpanderBuilder.addNodeFilter(nodeFilter);
			pathExpanderBuilder = pathExpanderBuilder.addNodeFilter(nodeFilter);
			pathExpanderBuilder = pathExpanderBuilder.addNodeFilter(nodeFilter);

			pathExpander = pathExpanderBuilder.build();
		}
	}

	@Override
	public void run() {
		try
		{
			System.out.println("Processing for TargetPaperId: " + targetPaperID);
			Map<String, Double> topicIDtoRWProabilityMap1 = new HashMap<String,Double>(this.findKeyNodesBetweenQueryAndTargetPaper(queryPaperIDs, targetPaperID));
			author.paperIDToKeyTopicPathMap.put(targetPaperID, new KeyTopicPath(author, paper, topicIDtoRWProabilityMap1));
			System.out.println("DONE! Processing for TargetPaperId: " + targetPaperID);
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			KeyNodeFinder.threadCompleted();
		}

	}

	void setData(List<String> queryPaperIDs, String targetPaperID, Author author, Paper paper)
	{
		this.queryPaperIDs = queryPaperIDs;
		this.targetPaperID = targetPaperID;
		this.author = author;
		this.paper = paper;
	}


	public Map<String, Double> findKeyNodesBetweenQueryAndTargetPaper(List<String> queryPaperIDs, String targetPaperID)
	{

		Map<String, Double> topicIDToRandomWalkProbilityMap = new HashMap<String, Double>();

		try (Transaction tx=dbService.beginTx()) 
		{
			Node targetPaperNode = dbService.findNode(Label.label("paper"), "acmID", targetPaperID);
			if(targetPaperNode != null)
			{
				double targetPaperRWProability = 0;
				for(String queryPaperID : queryPaperIDs)
				{
					//System.out.println("Start QueryPaperID: " + queryPaperID);
					try
					{
						//String paperOutput = String.format("\n\nQuery Paper ACMID: %s, TargetPaper ACMID: %s", queryPaperID, targetPaperID);
						//System.out.println(paperOutput);

						Node queryPaperNode = dbService.findNode(Label.label("paper"), "acmID", queryPaperID);


						//System.out.println("QueryPaperNode: " + queryPaperNode);
						//System.out.println("TargetPaperNode: " + targetPaperNode);

						//PathExpander<Object> pathExpander = PathExpanders.allTypesAndDirections();

						if(queryPaperNode != null)
						{

							PathFinder<Path> allPathFinder = GraphAlgoFactory.allSimplePaths(this.pathExpander,  Constants.MAX_TREE_DEPTH);

//							PathFinder<WeightedPath> allPathFinder = GraphAlgoFactory.dijkstra(
//									PathExpanders.forConstantDirectionWithTypes(RelationshipType.withName("cite"),
//											RelationshipType.withName("writtenby"),
//											RelationshipType.withName("relevant"),
//											RelationshipType.withName("contribute")), "weight", 100); //(this.pathExpander, 6);
							//System.out.println("Getting Paths...");

							Iterable<Path> allPaths = allPathFinder.findAllPaths(queryPaperNode, targetPaperNode);

							//System.out.println("Got Paths...");


							if (allPaths != null)
							{
								//								for(Path path : allPaths)
								//								{
								//									System.out.println(path.toString());
								//								}

								for(Path path : allPaths)
								{
									//System.out.println("Processing path- " + path.toString());
									Iterable<Relationship> relationships = path.relationships();
									double randomWalkProbability = 1.0;
									for(Relationship relationship : relationships)
									{
										randomWalkProbability = randomWalkProbability * (Double)(relationship.getProperty("weight"));
										if (randomWalkProbability <= 0)
										{
											break;
										}
									}

									if(randomWalkProbability > 0)
									{
										Iterable<Node> nodes = path.nodes();
										for(Node node : nodes)
										{
											if(node.hasLabel(Label.label("topic")))
											{
												String topicID = (String)node.getProperty("id");
												double existingScore = topicIDToRandomWalkProbilityMap.getOrDefault(topicID, 0.0);

												double tf = randomWalkProbability;
												double relatedPaperCount  = node.getDegree(RelationshipType.withName("contribute"));
												double idf = 1 + Math.log(Constants.ACM_PAPER_COUNT/relatedPaperCount);
												double score = tf * idf;
												double newScore = existingScore + score;
												topicIDToRandomWalkProbilityMap.put(topicID, newScore);
											}
										}
										//String outputPathLine = String.format("Path: %s, RWProbability: %f", path.toString(), randomWalkProbability);
										targetPaperRWProability += randomWalkProbability;
									}



									//System.out.println("Processing done- " + path.toString());
								}
							}


						}
						else
						{
							System.out.println("Can not find node for QueryPaper: " + queryPaperID);
						}
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					//System.out.println("End1 QueryPaperID: " + queryPaperID);

				}

				//System.out.println("Putting in Map!");
				targetPaperIdtoPathRWProbability.put(targetPaperID, targetPaperRWProability);
				//System.out.println("DONE-Putting in Map!");

			}
			else
			{
				System.out.println("Can not find node for TargetPaper: " + targetPaperID);
			}

			tx.success();
			tx.close();

		}

		// Filter for top 10 keywords for the paper
		if(topicIDToRandomWalkProbilityMap != null & 
				topicIDToRandomWalkProbilityMap.isEmpty() == false & 
				topicIDToRandomWalkProbilityMap.size() > 11)
		{
			topicIDToRandomWalkProbilityMap = getTop10Entries(topicIDToRandomWalkProbilityMap);
		}

		return topicIDToRandomWalkProbilityMap;

	}

	public <K, V extends Comparable<? super V>> Map<K, V> getTop10Entries(Map<K, V> map) {
		return map.entrySet()
				.stream()
				.sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
				.limit(10)
				.collect(Collectors.toMap(
						Map.Entry::getKey, 
						Map.Entry::getValue, 
						(e1, e2) -> e1, 
						LinkedHashMap::new
						));
	}


	public Thread start () {
		if(databaseDirectory == null)
		{
			databaseDirectory = new File(Constants.NEO_GRAPH_DB_PATH);
			dbService =  new GraphDatabaseFactory().newEmbeddedDatabase(databaseDirectory);
		}
		//System.out.println("Starting new thread");
		if (t == null) {
			t = new Thread(this);
			t.start ();
		}
		return t;
	}

	public static void cleanup()
	{
		dbService.shutdown();
		databaseDirectory = null;
		dbService = null;
	}

}
