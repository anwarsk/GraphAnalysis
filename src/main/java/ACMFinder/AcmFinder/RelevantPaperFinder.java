package ACMFinder.AcmFinder;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

public class RelevantPaperFInder {
	
//	public Map<String, Integer>
	
	public void getRelevantPaperIds(String AuthorID)
	{
		List<String> relevantTopics = new ArrayList<String>();
		
		
		Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "password" ) );
		Session session = driver.session();

		//session.run( "CREATE (a:Person {name:'Arthur', title:'King'})" );

		System.out.println("*** Finding Relevant Papers for Author: " + AuthorID + " ***");
		
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
		
		session.close();
		driver.close();
	}

}
