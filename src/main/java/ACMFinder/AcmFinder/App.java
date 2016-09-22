package ACMFinder.AcmFinder;

import java.util.List;

import ACMFinder.constants.Constants;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        RelevantPaperFinder finder = new RelevantPaperFinder();
        List<String> authorPapers = finder.getPapersWrittenByAuthor(Constants.AUTHOR_ID);
        List<String> targetPapers = finder.getTargetPapers(Constants.AUTHOR_ID);
        
        System.out.println("AuthorPapers-");
        System.out.println(authorPapers);
        
        System.out.println("\nTargetPapers-");
        System.out.println(targetPapers);
    }
}
