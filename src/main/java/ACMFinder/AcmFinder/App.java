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
        RelevantPaperFinder finder = new RelevantPaperFinder();
        finder.getRelevantPaperIds(Constants.AUTHOR_ID);
    }
}
