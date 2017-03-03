package summarizer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class TestSummary {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
		String path = "/home/anwar/GraphOutput/Mangu/Paper5.txt";
		
		List<String> lines = Files.readAllLines(Paths.get(path));
		String fullText = String.join("\n", lines);
		String query = "In-situ studies, design, ESM, ubicomputing, usability, experience sampling, PDA, alert, sonal server team, iESP"; 
		//String query = "awareness, ubiquitous computing, light-weight interaction, aging, visualization, home";
		
		TextSummarizer textSummarizer = new TextSummarizer();
		System.out.println(String.join("\n ", textSummarizer.getListOfTop5Sentences(query, fullText)));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		

	}

}
