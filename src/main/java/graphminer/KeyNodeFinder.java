package graphminer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.graphalgo.GraphAlgoFactory;
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


	public void findKeyNodesForAuthorAndConference(Author author, List<Paper> conferencePapers)
	{
		assert author !=null : "Invalid author";
		assert conferencePapers != null & !conferencePapers.isEmpty() : "Invalid list of conference papers.";

		List<String> queryPaperIds = author.writtenPaperACMIds;

		int batchSize = 16;
		int processedPapers = 0;
		PathFinderHelper pathFinderHelper;
		while(processedPapers < conferencePapers.size())
		{
			try
			{
				List<Thread> threadList = new ArrayList<Thread>();
				int lastIndex = processedPapers+batchSize;
				if(lastIndex > conferencePapers.size())
				{
					lastIndex = conferencePapers.size();
				}

				for (Paper conferencePaper : conferencePapers.subList(processedPapers, lastIndex))
				{

					String targetPaperID = conferencePaper.acmID;
					pathFinderHelper = new PathFinderHelper();
					pathFinderHelper.setData(queryPaperIds, targetPaperID, author, conferencePaper);
					threadList.add(pathFinderHelper.start());
				}

				for(Thread thread: threadList)
				{
					try 
					{
						thread.join();
					} 
					catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				processedPapers = lastIndex;
				PathFinderHelper.cleanup();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}

		Map<String, Double> targetPaperIdToPathRWProbability = PathFinderHelper.targetPaperIdtoPathRWProbability;
		if(targetPaperIdToPathRWProbability != null & 
				targetPaperIdToPathRWProbability.isEmpty() == false)
		{
			if(targetPaperIdToPathRWProbability.size() > 10 )
			{
				author.paperIDToRWProability.putAll(this.getTop10Entries(targetPaperIdToPathRWProbability));
			}
			else
			{
				author.paperIDToRWProability.putAll(targetPaperIdToPathRWProbability);
			}
		}
		//KeyTopicPathFinder.dbService.shutdown();
	}

	public <K, V extends Comparable<? super V>> Map<K, V> getTop10Entries(Map<K, V> map) {
		return map.entrySet()
				.stream()
				.sorted(Map.Entry.comparingByValue(/*Collections.reverseOrder()*/))
				.limit(10)
				.collect(Collectors.toMap(
						Map.Entry::getKey, 
						Map.Entry::getValue, 
						(e1, e2) -> e1, 
						LinkedHashMap::new
						));
	}



}

class ACMNodeFilter implements Predicate<Node>{

	@Override
	public boolean test(Node testNode) {
		// TODO Auto-generated method stub
		boolean isNodeMatch = false;

		if(testNode.hasLabel(Label.label("author")) | 
				testNode.hasLabel(Label.label("topic")) | 
				testNode.hasLabel(Label.label("paper")) )
		{
			isNodeMatch = true;
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
	public static Map<String, Double> targetPaperIdtoPathRWProbability = new HashMap<String, Double>();
	private PathExpander<Object> pathExpander = null;

	public PathFinderHelper() {
		if(pathExpander == null)
		{
			ACMNodeFilter nodeFilter = new ACMNodeFilter();
			PathExpanderBuilder pathExpanderBuilder = PathExpanderBuilder.empty();
			pathExpanderBuilder.addNodeFilter(nodeFilter);
			pathExpanderBuilder.addNodeFilter(nodeFilter);
			pathExpanderBuilder.addNodeFilter(nodeFilter);
			pathExpanderBuilder.addNodeFilter(nodeFilter);
			pathExpanderBuilder.addNodeFilter(nodeFilter);

			pathExpander = pathExpanderBuilder.build();
		}
	}

	@Override
	public void run() {
		System.out.println("Processing for TargetPaperId: " + targetPaperID);
		Map<String, Double> topicIDtoRWProabilityMap1 = this.findKeyNodesBetweenQueryAndTargetPaper(queryPaperIDs, targetPaperID);
		author.paperIDToKeyTopicPathMap.put(targetPaperID, new KeyTopicPath(author, paper, topicIDtoRWProabilityMap1));
		//String paperOutput = String.format("\n**END**Query Paper ACMID: %s, TargetPaper ACMID: %s", "querys", targetPaperID);
		//System.out.println(paperOutput);

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
				for(String queryPaperID : queryPaperIDs)
				{
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

							org.neo4j.graphalgo.PathFinder<Path> allPathFinder = GraphAlgoFactory.allSimplePaths(this.pathExpander, 4);

							Iterable<Path> allPaths = allPathFinder.findAllPaths(queryPaperNode, targetPaperNode);
							double targetPaperRWProability = 0;
							if (allPaths != null)
							{
								for(Path path : allPaths)
								{
									Iterable<Relationship> relationships = path.relationships();
									double randomWalkProbability = 1.0;
									for(Relationship relationship : relationships)
									{
										randomWalkProbability = randomWalkProbability * (Double)(relationship.getProperty("weight"));
										if (randomWalkProbability < 0.0000001)
										{
											break;
										}
									}

									if(randomWalkProbability > 0.0000001)
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

									}

									targetPaperRWProability += randomWalkProbability;
								}
							}

							targetPaperIdtoPathRWProbability.put(targetPaperID, targetPaperRWProability);
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
				}

			}
			else
			{
				System.out.println("Can not find node for TargetPaper: " + targetPaperID);
			}

			tx.success();
			tx.close();

		}

		// Filter for top 10 keywords for the paper
		topicIDToRandomWalkProbilityMap = getTop10Entries(topicIDToRandomWalkProbilityMap);


		return topicIDToRandomWalkProbilityMap;

	}

	public <K, V extends Comparable<? super V>> Map<K, V> getTop10Entries(Map<K, V> map) {
		return map.entrySet()
				.stream()
				.sorted(Map.Entry.comparingByValue(/*Collections.reverseOrder()*/))
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
