package ACMFinder.AcmFinder;

import ACMFinder.constants.Constants;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        RelevantPaperFInder finder = new RelevantPaperFInder();
        finder.getRelevantPaperIds(Constants.AUTHOR_ID);
    }
}
