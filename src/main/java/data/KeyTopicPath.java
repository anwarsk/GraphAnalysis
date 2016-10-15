package data;

import java.util.Map;

public class KeyTopicPath {
	
	public Author author;
	public Paper paper;
	public Map<String, Double> topicIDtoProbabilityMap;

	public KeyTopicPath(Author author, Paper paper, Map<String, Double> topicIDtoProbabilityMap)
	{
		this.author = author;
		this.paper = paper;
		this.topicIDtoProbabilityMap = topicIDtoProbabilityMap;
	}
}
