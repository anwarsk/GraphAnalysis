package constant;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;

public class Constants {

	public static String AUTHOR_ID ;//= "A_554";  // Have total 157 papers 

	//public static String[] AUTHOR_FIRST_NAMES = new String[]{"bruce", "chengxiang", "jiawei", "johan", "kevin"};
	//public static String[] AUTHOR_LAST_NAMES = new String[]{"croft", "zhai", "han", "bollen", "crowston"};

	public static String[] AUTHOR_FIRST_NAMES;// = new String[]{"johan", "kevin"};
	public static String[] AUTHOR_LAST_NAMES;// = new String[]{"bollen", "crowston"};

	//public static String SIGIR_2006_COFERENCE;// = "3557";
	//public static String SIGIR_2008_COFERENCE;// = "1172";

	public static long ACM_PAPER_COUNT;// = 249381;

	//public static String NEO_GRAPH_DB_PATH = "/nfs/nfs4/home/anshaikh/GraphMining/graph.dbacm240/";

	//public static String OUTPUT_FILE_PATH = "/nfs/nfs4/home/anshaikh/GraphMining/Output/%s_%s.tsv";

	public static String NEO_GRAPH_DB_PATH;// = "/N/u/anshaikh/BigRed2/GraphMining/graph.dbacm240/";
	public static String OUTPUT_FILE_PATH;// = "N/u/anshaikh/BigRed2/GraphMining/Output/%s_%s.tsv";
	
	public static Map<String, String[]> CONFERENCE_LIST;

	//public static String NEO_GRAPH_DB_PATH = "/home/anwar/GraphOutput/graph.dbacm240/";

	//public static String OUTPUT_FILE_PATH = "/home/anwar/GraphOutput/Output/%s_%s.tsv";
	
	public static int MAXIMUM_THREAD_COUNT;
	
	public static int MAX_TREE_DEPTH;

	static
	{
		try 
		{
			Properties configuration = new Properties();

			configuration.load(new FileReader(new File("configuration.properties")));
			

			AUTHOR_FIRST_NAMES = configuration.getProperty("AUTHOR_FIRST_NAMES").split(", ");
					
			AUTHOR_LAST_NAMES = configuration.getProperty("AUTHOR_LAST_NAMES").split(", ");

			CONFERENCE_LIST =  new HashMap<String, String[]>();
			
			for (String conferenceDetails : configuration.getProperty("CONFERENCE_LIST").split(";" ))
			{
				String[] conference = conferenceDetails.split(", ");
				CONFERENCE_LIST.put(conference[0], ArrayUtils.remove(conference, 0));
			}
			
			
			ACM_PAPER_COUNT = Integer.parseInt(configuration.getProperty("PAPER_COUNT"));
			

			NEO_GRAPH_DB_PATH = configuration.getProperty("NEO_GRAPH_DB_PATH");
			OUTPUT_FILE_PATH = configuration.getProperty("OUTPUT_FILE_PATH");
			
			MAXIMUM_THREAD_COUNT = Integer.parseInt(configuration.getProperty("MAXIMUM_THREAD_COUNT"));
			
			MAX_TREE_DEPTH = Integer.parseInt(configuration.getProperty("MAX_TREE_DEPTH"));
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
