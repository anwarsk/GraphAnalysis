package ACMFinder.AcmFinder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

public class RelevantPaperFinder {

	//	public Map<String, Integer>

	/**
	 * This function returns list of papers written by the author.
	 * @param AuthorID
	 * @return
	 */
	public List<String> getPapersWrittenByAuthor(String AuthorID)
	{
		List<String> papersByAuthor = new ArrayList<String>();

		Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "password" ) );
		Session session = driver.session();

		//session.run( "CREATE (a:Person {name:'Arthur', title:'King'})" );

		//System.out.println("*** Finding Relevant Papers for Author: " + AuthorID + " ***");

		StatementResult result = session.run( "MATCH (n:paper)-[r:writtenby]->(a:author) WHERE a.id = '"+ AuthorID +"' RETURN n.acmID as ID LIMIT 200");
		//StatementResult result = session.run( "MATCH (n:paper) -WHERE a.name = 'Arthur' RETURN a.name AS name, a.title AS title" );

		//System.out.println("\n\nPapers Written By the Author: ");
		while ( result.hasNext() )
		{
			Record record = result.next();
			String paperID = record.get( "ID" ).asString();
			papersByAuthor.add(paperID);	
		}


		session.close();
		driver.close();

		return papersByAuthor;

	}

	public List<String> getTargetPapers(String authorID)
	{
		List<String> targetPapers = new ArrayList<String>();

		targetPapers.addAll(this.getTargetPapersAtDepth(authorID, 3, 3));
		targetPapers.addAll(this.getTargetPapersAtDepth(authorID, 4, 3));
		targetPapers.addAll(this.getTargetPapersAtDepth(authorID, 7, 3));
		targetPapers.addAll(this.getTargetPapersAtDepth(authorID, 8, 3));
		targetPapers.addAll(this.getTargetPapersAtDepth(authorID, 10, 3));

		return targetPapers;

	}

	private List<String> getTargetPapersAtDepth(String authorID, int depth, int paperCount)
	{
		List<String> targetPapers = new ArrayList<String>();

		String query = "MATCH (a:author {id:'%s'})<-[:writtenby]-(p1:paper)-[*%d]-(p2:paper) RETURN p2.acmID as ID LIMIT 50";
		query = String.format(query, authorID, depth, paperCount);

		Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic( "neo4j", "password" ) );
		Session session = driver.session();

		StatementResult result = session.run(query);

		while (result.hasNext())
		{
			Record record = result.next();
			String paperID = record.get( "ID" ).asString();
			targetPapers.add(paperID);	
		}



		session.close();
		driver.close();

		// Select papers randomly at different depths
		Collections.shuffle(targetPapers);
		targetPapers = targetPapers.subList(0, paperCount);

		return targetPapers;
	}


	public void getRelevantPaperIds(String AuthorID)
	{
		List<String> relevantTopics = new ArrayList<String>();


		Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "password" ) );
		Session session = driver.session();

		//session.run( "CREATE (a:Person {name:'Arthur', title:'King'})" );

		System.out.println("*** Finding Relevant Papers for Author: " + AuthorID + " ***");

		//TO-TEST
		String pathQuery = "MATCH p=(a:author {id:'A_554'})<-[:writtenby]-(p1:paper)-[*5..7]-(p2:paper) RETURN p LIMIT 25";
		StatementResult result = session.run(pathQuery);
		while ( result.hasNext() )
		{
			Record record = result.next();
			System.out.println("Record: " + record);
		}

		/**



		StatementResult result = session.run( "MATCH (n:paper)-[r:writtenby]->(a:author) WHERE a.id = '"+ AuthorID +"' RETURN n.acmID as ID LIMIT 30");
		//StatementResult result = session.run( "MATCH (n:paper) -WHERE a.name = 'Arthur' RETURN a.name AS name, a.title AS title" );

		System.out.println("\n\nPapers Written By the Author: ");
		while ( result.hasNext() )
		{
		    Record record = result.next();
		    String paperID = record.get( "ID" ).asString();

		    StatementResult result1 = session.run( "MATCH (n:paper)-[r:relevant]->(t:topic) WHERE n.acmID = '"+ paperID +"' RETURN t.id as TopicID LIMIT 5");
		    System.out.print(paperID + ", ");

		    while ( result1.hasNext() )
			{
			    Record record1 = result1.next();
			    String topicID = record1.get( "TopicID" ).asString();
			    relevantTopics.add(topicID);
			}
		}

		System.out.println("\n\n(Relevant Topics for Author: \n" + relevantTopics.toString());


		System.out.println("\n\nRelevant Papers to the Author: ");
		for (String topic : relevantTopics) {

			StatementResult result2 = session.run( "MATCH (n:paper)-[r:relevant]->(t:topic) WHERE t.id = '"+ topic +"' RETURN n.acmID as paperID LIMIT 2");

		    while ( result2.hasNext() )
			{
			    Record record = result2.next();
			    String paperID = record.get( "paperID" ).asString();

			    System.out.print(paperID + ", ");
			}
		}
		 **/
		session.close();
		driver.close();
	}

}
