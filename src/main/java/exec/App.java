package exec;

import java.io.FileNotFoundException;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import constant.Constants;
import data.Author;
import data.KeyTopicPath;
import data.Paper;
import dbLayer.SQLAccessLayer;
import graphminer.KeyNodeFinder;


public class App 
{
	public static void main( String[] args ) throws FileNotFoundException
	{


		List<Author> authors = new ArrayList<Author>();
		SQLAccessLayer accessLayer = new SQLAccessLayer();

		/**
		List<String> paperTitles =  new ArrayList<String>();
		paperTitles.add("Elicitation of term relevance feedback");
		paperTitles.add("The TIJAH XML information retrieval system");
		paperTitles.add("Concept-based biomedical text retrieval");
		paperTitles.add("A platform for Okapi-based contextual information retrieval");
		paperTitles.add("Adapting ranking SVM to document retrieval");
		paperTitles.add("Unity");
		paperTitles.add("MathFind");
		paperTitles.add("LDA-based document models for ad-hoc retrieval");
		paperTitles.add("History repeats itself");
		paperTitles.add("Hybrid index maintenance for growing text collections");


		for (String papertitle : paperTitles)
		{
			Paper paper = accessLayer.getPaperWithTitle(papertitle);
			PrintWriter outputFileWriter = new PrintWriter("/home/anwar/GraphOutput/Papers" + papertitle + ".txt");
			outputFileWriter.write(paper.abstractText);
			outputFileWriter.close();
		}

		 **/
		/**
		/** TO-TEST:
		Set<String> testSet = new HashSet<String>();
		testSet.add("T_606");
		testSet.add("T_607");
		testSet.add("T_608");
		System.out.println("testSet :" + accessLayer.getListOfKeywords(testSet));
		 **/

		for (int authorIndex = 0; authorIndex < Constants.AUTHOR_FIRST_NAMES.length; authorIndex++)
		{
			Author author = new Author();
			List<String> authorIDs = accessLayer.getListOfAuthorIDs(Constants.AUTHOR_FIRST_NAMES[authorIndex], Constants.AUTHOR_LAST_NAMES[authorIndex]);

			author.firstName = Constants.AUTHOR_FIRST_NAMES[authorIndex];
			author.lastName = Constants.AUTHOR_LAST_NAMES[authorIndex];
			author.ids = authorIDs;

			System.out.println("\nFetching Papers for Author- " + author.firstName);
			List<String> authorPapers = accessLayer.getPapersWrittenByAuthor(authorIDs);
			author.writtenPaperACMIds.addAll(authorPapers);
			System.out.println(String.format("Retrieved Total %d papers.", authorPapers.size()));


			authors.add(author);
		}


		for(String conferenceName : Constants.CONFERENCE_LIST.keySet())
		{
			System.out.println("Fetching Papers for: " + conferenceName);
			List<Paper> conferencePapers = new ArrayList<Paper>();

			for(String conferenceId : Constants.CONFERENCE_LIST.get(conferenceName))
			{

				conferencePapers.addAll(accessLayer.getListOfPapersforConference(conferenceId));
				//List<Paper> sigr2008Papers = accessLayer.getListOfPapersforConference(Constants.SIGIR_2008_COFERENCE);

			}
			KeyNodeFinder keyNodeFinder = new KeyNodeFinder();

			for(Author author : authors)
			{
				System.out.println("Processing conference for author- " + author.firstName);

				keyNodeFinder.findKeyNodesForAuthorAndConference(author, conferencePapers);

				//			System.out.println("**** SUMMARY ****");
				//			System.out.println("\n\n\nAuthorName: " + author.firstName);
				//			System.out.println("Top10Papers: " + author.paperIDToRWProability);
				//			System.out.println("Top10");

				String outputFile = String.format(Constants.OUTPUT_FILE_PATH, author.firstName, author.lastName);

				PrintWriter outputFileWriter = new PrintWriter(outputFile);
				outputFileWriter.print(String.format("\n\t *** TOP-10 PAPERS FOR %s %s in SIGIR2006 Conference ***\n",author.firstName, author.lastName ));

				int paperIndex = 1;
				for(String paperID : author.paperIDToRWProability.keySet())
				{
					try
					{
						KeyTopicPath keyTopicPath = author.paperIDToKeyTopicPathMap.get(paperID);
						if(keyTopicPath != null)
						{
							Paper paper = keyTopicPath.paper;
							Set<String> topicIds = keyTopicPath.topicIDtoProbabilityMap.keySet();
							String keywords = "";
							String outputLine = "%d\t%s\t%s\t%s";
							if(topicIds != null & topicIds.isEmpty() == false)
							{
								List<String> topics = accessLayer.getListOfKeywords(topicIds);
								keywords = org.apache.commons.lang3.StringUtils.join(topics, ", ");
							}

							outputLine = String.format(outputLine, paperIndex, keywords, paper.title, paper.acmID);
							outputFileWriter.println(outputLine);
							paperIndex++;
						}
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
				}

				outputFileWriter.close();



			}

		}
		//**/

		System.out.println("Processsing is Completed !!!");

	}



}
