package ACMFinder.AcmFinder;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ACMFinder.constants.Constants;
import sigir.Author;
import sigir.KeyTopicPath;
import sigir.Paper;
import sigir.SQLAccessLayer;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void main( String[] args ) throws FileNotFoundException
	{
		//        RelevantPaperFinder finder = new RelevantPaperFinder();
		//        List<String> authorPapers = finder.getPapersWrittenByAuthor(Constants.AUTHOR_ID);
		//        List<String> targetPapers = finder.getTargetPapers(Constants.AUTHOR_ID);
		//        
		//        // Select random 10 papers out of 100
		//        Collections.shuffle(authorPapers);
		//        authorPapers = authorPapers.subList(0, 10);
		//        
		//        System.out.println("AuthorPapers-");
		//        System.out.println(authorPapers);
		//        
		//        System.out.println("\nTargetPapers-");
		//        System.out.println(targetPapers);
		//        
		//        //PathFinder pathFinder = new PathFinder();
		//        //pathFinder.getPathBetweenNodes(Constants.AUTHOR_ID, targetPapers.get(0));
		//        
		//        KeyNodeFinder keyNodeFinder = new KeyNodeFinder();
		//        keyNodeFinder.findKeyNodesInPath(authorPapers, targetPapers);
		//  



		List<Author> authors = new ArrayList<Author>();
		//RelevantPaperFinder finder = new RelevantPaperFinder();
		SQLAccessLayer accessLayer = new SQLAccessLayer();


		Set<String> testSet = new HashSet<String>();
		testSet.add("T_606");
		testSet.add("T_607");
		testSet.add("T_608");

		System.out.println("testSet :" + accessLayer.getListOfKeywords(testSet));


		for (int authorIndex = 0; authorIndex < Constants.AUTHOR_FIRST_NAMES.length; authorIndex++)
		{
			//			int authorIndex = 2;
			Author author = new Author();
			List<String> authorIDs = accessLayer.getListOfAuthorIDs(Constants.AUTHOR_FIRST_NAMES[authorIndex], Constants.AUTHOR_LAST_NAMES[authorIndex]);

			author.firstName = Constants.AUTHOR_FIRST_NAMES[authorIndex];
			author.lastName = Constants.AUTHOR_LAST_NAMES[authorIndex];
			author.ids = authorIDs;

			for(String authorID : authorIDs)
			{
				List<String> authorPapers = accessLayer.getPapersWrittenByAuthor(authorID);

				author.writtenPaperACMIds.addAll(authorPapers);
			}
			List<String> toRemove = new ArrayList<String>();
			toRemove.add("null");
			toRemove.add("");
			toRemove.add(null);

			author.writtenPaperACMIds.removeAll(toRemove);
			authors.add(author);
			System.out.println("PAPERS: " + author.writtenPaperACMIds);
			System.out.println("Author Papers:\n" + author.writtenPaperACMIds.size());
		}


		List<Paper> sigr2006Papers = accessLayer.getListOfPapersforConference(Constants.SIGIR_2006_COFERENCE);
		//List<Paper> sigr2008Papers = accessLayer.getListOfPapersforConference(Constants.SIGIR_2008_COFERENCE);

		KeyNodeFinder keyNodeFinder = new KeyNodeFinder();

		for(Author author : authors)
		{
			keyNodeFinder.findKeyNodesForAuthorAndConference(author, sigr2006Papers);
			//writeAuthorDataToFile(author);

			System.out.println("**** SUMMARY ****");
			System.out.println("\n\n\nAuthorName: " + author.firstName);
			System.out.println("Top10Papers: " + author.paperIDToRWProability);
			//System.out.println("Top10");
		}

		for (Author author : authors)
		{
			String outputFile = String.format(Constants.OUTPUT_DIRECTORY, author.firstName, author.lastName) ;
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

						List<String> topics = accessLayer.getListOfKeywords(keyTopicPath.topicIDtoProbabilityMap.keySet());
						String keywords = org.apache.commons.lang3.StringUtils.join(topics, ", ");

						String outputLine = "%d\t%s\t%s";
						outputLine = String.format(outputLine, paperIndex, keywords, paper.title);
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
		//
		//		ObjectOutputStream oos = null;
		//		FileOutputStream fout = null;
		//		try{
		//			 fout = new FileOutputStream("/home/anwar/outputFile.ser", true);
		//			 oos = new ObjectOutputStream(fout);
		//			oos.writeObject(authors);
		//			oos.close();
		//		} catch (Exception ex) {
		//			ex.printStackTrace();
		//		}
		System.out.println("Processsing is done!!!");

	}



}
