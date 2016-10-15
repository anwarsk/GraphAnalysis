package data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Author {
	
	public String firstName;
	public String lastName;
	public List<String> ids;
	public List<String> writtenPaperACMIds;
	public Map<String, KeyTopicPath> paperIDToKeyTopicPathMap;
	public Map<String, Double> paperIDToRWProability;
	
	public Author()
	{
		writtenPaperACMIds = new ArrayList<String>();
		paperIDToKeyTopicPathMap = new HashMap<String, KeyTopicPath>();
		paperIDToRWProability = new HashMap<String, Double>();
	}

}
