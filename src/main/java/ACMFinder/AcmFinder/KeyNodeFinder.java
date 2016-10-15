package ACMFinder.AcmFinder;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import ACMFinder.constants.Constants;
import sigir.Author;
import sigir.KeyTopicPath;
import sigir.Paper;

public class KeyNodeFinder {


	public void findKeyNodesForAuthorAndConference(Author author, List<Paper> conferencePapers)
	{
		List<String> queryPaperIds = author.writtenPaperACMIds;
		//		for (Paper conferencePaper : conferencePapers)
		//		{
		//			String targetPaperID = conferencePaper.acmID;
		//			Map<String, Double> topicIDtoRWProabilityMap = this.findKeyNodesBetweenQueryAndTargetPaper(queryPaperIds, targetPaperID);
		//			KeyTopicPath keyTopicPath = new KeyTopicPath(author, conferencePaper, topicIDtoRWProabilityMap);
		//			author.paperIDToKeyTopicPathMap.put(targetPaperID, keyTopicPath);
		//		}

		int batchSize = 16;
		int processedPapers = 0;
		KeyTopicPathFinder finder;
		while(processedPapers < conferencePapers.size())//conferencePapers.size())
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
					finder = new KeyTopicPathFinder();
					finder.setData(queryPaperIds, targetPaperID, author, conferencePaper);
					threadList.add(finder.start());
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
				KeyTopicPathFinder.cleanup();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}

		if(KeyTopicPathFinder.paperIDtoRWProbability != null & KeyTopicPathFinder.paperIDtoRWProbability.isEmpty() == false)
		{
			author.paperIDToRWProability.putAll(this.getTop10Entries(KeyTopicPathFinder.paperIDtoRWProbability));
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


	/*	map
	public void findKeyNodesInPath(List<String> queryPaperIDs, List<String> targetPaperIDs) throws FileNotFoundException
	{
		File databaseDirectory = new File("/media/anwar/825ED72B5ED716AF/Work/Database/graph.dbacm240/");
		GraphDatabaseService dbService =  new GraphDatabaseFactory().newEmbeddedDatabase(databaseDirectory);
		Map<Node, Double> nodeToRandomWalkProbilityMap = new HashMap<Node, Double>();

		String outputPathFile = "/home/anwar/graph_path_output.txt";
		PrintWriter outputPathFileWriter = new PrintWriter(outputPathFile);
		outputPathFileWriter.println("**** FINDING KEY NODES ****");
		outputPathFileWriter.println("\nAuthorID: " + Constants.AUTHOR_ID);
		outputPathFileWriter.println("\nQuery Paper (acmID): " + queryPaperIDs.toString());
		outputPathFileWriter.println("\nTarget Paper (acmID): " + targetPaperIDs.toString());

		try (Transaction tx=dbService.beginTx()) 
		{
			for(String queryPaperID : queryPaperIDs)
			{
				for(String targetPaperID : targetPaperIDs)
				{
					String paperOutput = String.format("\n\nQuery Paper ACMID: %s, TargetPaper ACMID: %s", queryPaperID, targetPaperID);
					System.out.println(paperOutput);
					outputPathFileWriter.println(paperOutput);

					Node queryPaperNode = dbService.findNode(Label.label("paper"), "acmID", queryPaperID);
					Node targetPaperNode = dbService.findNode(Label.label("paper"), "acmID", targetPaperID);

					System.out.println("QueryPaperNode: " + queryPaperNode);
					System.out.println("TargetPaperNode: " + targetPaperNode);

					PathExpander<Object> pathExpander = PathExpanders.allTypesAndDirections();

					org.neo4j.graphalgo.PathFinder<Path> allPathFinder = GraphAlgoFactory.allPaths(pathExpander, 4);

					Iterable<Path> allPaths = allPathFinder.findAllPaths(queryPaperNode, targetPaperNode);
					if (allPaths != null)
					{
						for(Path path : allPaths)
						{
							Iterable<Relationship> relationships = path.relationships();
							double randomWalkProbability = 1.0;
							for(Relationship relationship : relationships)
							{
								randomWalkProbability = randomWalkProbability * (Double)(relationship.getProperty("weight"));
								if (randomWalkProbability < 0.000001)
								{
									break;
								}
							}

							if(randomWalkProbability > 0.000001)
							{
								Iterable<Node> nodes = path.nodes();
								for(Node node : nodes)
								{
									double existingProbability = nodeToRandomWalkProbilityMap.getOrDefault(node, 0.0);
									double newProbability = existingProbability + randomWalkProbability;
									nodeToRandomWalkProbilityMap.put(node, newProbability);
								}
								String outputPathLine = String.format("Path: %s, RWProbability: %f", path.toString(), randomWalkProbability);
								outputPathFileWriter.println(outputPathLine);
							}

						}
					}

				}
			}

			outputPathFileWriter.close();

			String outputFile = "/home/anwar/graph_output.csv";
			PrintWriter outputFileWriter = new PrintWriter(outputFile);

			for(Node node : nodeToRandomWalkProbilityMap.keySet())
			{
				long nodeID = node.getId();
				String label = node.getLabels().iterator().next().name();
				double probability = nodeToRandomWalkProbilityMap.get(node);
				String outputLine = String.format("%d,%s,%f", nodeID, label, probability);
				outputFileWriter.println(outputLine);
			}

			outputFileWriter.close();

			tx.success();
		}



		dbService.shutdown();


	}
	 */
}

class KeyTopicPathFinder implements Runnable{

	public List<String> queryPaperIDs;
	public String targetPaperID;
	public Author author = null;
	public Paper paper = null;
	private Thread t = null;
	public static File databaseDirectory = null;
	public static GraphDatabaseService dbService =null;
	public static Map<String, Double> paperIDtoRWProbability = new HashMap<String, Double>();
	@Override
	public void run() {
		// TODO Auto-generated method stub
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
			for(String queryPaperID : queryPaperIDs)
			{
				try
				{
					String paperOutput = String.format("\n\nQuery Paper ACMID: %s, TargetPaper ACMID: %s", queryPaperID, targetPaperID);
					System.out.println(paperOutput);

					Node queryPaperNode = dbService.findNode(Label.label("paper"), "acmID", queryPaperID);
					Node targetPaperNode = dbService.findNode(Label.label("paper"), "acmID", targetPaperID);

					//System.out.println("QueryPaperNode: " + queryPaperNode);
					//System.out.println("TargetPaperNode: " + targetPaperNode);

					PathExpander<Object> pathExpander = PathExpanders.allTypesAndDirections();

					org.neo4j.graphalgo.PathFinder<Path> allPathFinder = GraphAlgoFactory.allSimplePaths(pathExpander, 4);

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
								if (randomWalkProbability < 0.000001)
								{
									break;
								}
							}

							if(randomWalkProbability > 0.000001)
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

					paperIDtoRWProbability.put(targetPaperID, targetPaperRWProability);
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}

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
		System.out.println("Starting new thread");
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
