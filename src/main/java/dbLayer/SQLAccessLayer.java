package dbLayer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import data.Paper;



public class SQLAccessLayer {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://rdc04.uits.iu.edu:3264/ACM";

	//  Database credentials
	static final String USERNAME = "summary_proj";
	static final String PASSWORD = "xpa25Rd";

	public List<String> getListOfAuthorIDs(String authorFirstName, String authorLastName)
	{
		
		assert authorFirstName != null & !authorFirstName.isEmpty(): "Invalid Author First Name" + authorFirstName;
		assert authorLastName != null & !authorLastName.isEmpty(): "Invalid Author Last Name" + authorFirstName;

		String query = "select id from author where first_name = '%s' and last_name='%s'";
		List<String> authorIDs = new ArrayList<String>();

		query = String.format(query, authorFirstName, authorLastName);
		System.out.println("Executing query:" + query);
		Connection conn = null;
		Statement stmt = null;
		try{
			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver").newInstance();



			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL,USERNAME,PASSWORD);

			//STEP 4: Execute a query
			//System.out.println("Creating statement...");
			stmt = conn.createStatement();

			String sql;
			sql = query;
			ResultSet rs = stmt.executeQuery(sql);

			//STEP 5: Extract data from result set
			while(rs.next()){
				//Retrieve by column name
				int id  = rs.getInt("id");

				authorIDs.add(String.valueOf(id));
				//Display values
				//System.out.println("ID: " + id);

			}
			//STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			conn.close();
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		}finally{
			//finally block used to close resources
			try{
				if(stmt!=null)
					stmt.close();
			}catch(SQLException se2){
			}// nothing we can do
			try{
				if(conn!=null)
					conn.close();
			}catch(SQLException se){
				se.printStackTrace();
			}//end finally try
		}//end try
		//System.out.println("Goodbye!");
		return authorIDs;
	}

	public List<Paper> getListOfPapersforConference(String conferenceId)
	{
		assert conferenceId != null & !conferenceId.isEmpty() : "Invalid conference ID- " + conferenceId;
		
		List<Paper> papers = new ArrayList<Paper>();

		String query = "select id, title, abstract_text from paper where jour_conf_id = '%s'";

		query = String.format(query, conferenceId);
		System.out.println("Executing query:" + query);
		Connection conn = null;
		Statement stmt = null;
		try{
			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver").newInstance();



			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL,USERNAME,PASSWORD);

			//STEP 4: Execute a query
			//System.out.println("Creating statement...");
			stmt = conn.createStatement();

			String sql;
			sql = query;
			ResultSet rs = stmt.executeQuery(sql);

			//STEP 5: Extract data from result set
			while(rs.next()){
				//Retrieve by column name
				
				int id  = rs.getInt("id");
				String title = rs.getString("title");
				String abstractText = rs.getString("abstract_text");
				
				//System.out.println(String.format("ID: %s   Title:%s", id, title));

				Paper paper = new Paper(id, title, abstractText, conferenceId);

				papers.add(paper);
				
				//Display values
				//System.out.println("ID: " + id);

			}
			//STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			conn.close();
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		}finally{
			//finally block used to close resources
			try{
				if(stmt!=null)
					stmt.close();
			}catch(SQLException se2){
			}// nothing we can do
			try{
				if(conn!=null)
					conn.close();
			}catch(SQLException se){
				se.printStackTrace();
			}//end finally try
		}//end try
		//System.out.println("Goodbye!");

		return papers;
	}
	
	
	public List<String> getListOfKeywords(Set<String> topics)
	{
		String query = "select keyword from keyword where id in (%s)";
		List<String> keywords = new ArrayList<String>();

		String topicList = StringUtils.join(topics, ",");
		topicList = topicList.replaceAll("T_", "");
		
		query = String.format(query, topicList);
		System.out.println("Executing query:" + query);
		Connection conn = null;
		Statement stmt = null;
		try{
			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver").newInstance();



			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL,USERNAME,PASSWORD);

			//STEP 4: Execute a query
			//System.out.println("Creating statement...");
			stmt = conn.createStatement();

			String sql;
			sql = query;
			ResultSet rs = stmt.executeQuery(sql);

			//STEP 5: Extract data from result set
			while(rs.next()){
				//Retrieve by column name
				String keyword  = rs.getString("keyword");

				keywords.add(String.valueOf(keyword));
				//Display values
				//System.out.println("ID: " + id);

			}
			//STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			conn.close();
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		}finally{
			//finally block used to close resources
			try{
				if(stmt!=null)
					stmt.close();
			}catch(SQLException se2){
			}// nothing we can do
			try{
				if(conn!=null)
					conn.close();
			}catch(SQLException se){
				se.printStackTrace();
			}//end finally try
		}//end try
		//System.out.println("Goodbye!");
		return keywords;
	}
	
	public List<String> getPapersWrittenByAuthor(String authorID)
	{
		assert authorID != null & !authorID.isEmpty() : "Invalid Author ID: " + authorID;
		
		String query = "select paper_id from paper_author where author_id = %s";
		List<String> paperIds = new ArrayList<String>();

	
		query = String.format(query, authorID);
		System.out.println("Executing query:" + query);
		Connection conn = null;
		Statement stmt = null;
		try{
			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver").newInstance();



			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL,USERNAME,PASSWORD);

			//STEP 4: Execute a query
			//System.out.println("Creating statement...");
			stmt = conn.createStatement();

			String sql;
			sql = query;
			ResultSet rs = stmt.executeQuery(sql);

			//STEP 5: Extract data from result set
			while(rs.next()){
				//Retrieve by column name
				String paper_id  = rs.getString("paper_id");

				paperIds.add(String.valueOf(paper_id));
				//Display values
				//System.out.println("ID: " + id);

			}
			//STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			conn.close();
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		}finally{
			//finally block used to close resources
			try{
				if(stmt!=null)
					stmt.close();
			}catch(SQLException se2){
			}// nothing we can do
			try{
				if(conn!=null)
					conn.close();
			}catch(SQLException se){
				se.printStackTrace();
			}//end finally try
		}//end try
		//System.out.println("Goodbye!");
		List<String> toRemove = new ArrayList<String>();
		toRemove.add("null");
		toRemove.add("");
		toRemove.add(null);
		
		paperIds.removeAll(toRemove);
		
		return paperIds;
	}
}
