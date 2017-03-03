package summarizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import diversity.MMR;
import diversity.ResultListSelector;
import diversity.kernel.Kernel;
import diversity.kernel.TFIDF;

public class TextSummarizer {

	public List<String> getListOfTop5Sentences(String queryKeywords, String text)
	{
		//System.out.println(String.format("Processing Query %s, Text-\n %s", queryKeywords, text));
		List<String> top5Sentences  = new ArrayList<String>();
		List<String> sentences = new ArrayList<String>();
		HashMap<String, String> sentencesMap = new HashMap<String, String>();

		String[] sentenceAraay = text.split("(?<=[a-z])\\.\\s+");
		Collections.addAll(sentences, sentenceAraay);
		
		
		for (String sentence : sentences)
		{
			sentencesMap.put(sentence, sentence);
		}

		Kernel TFIDF_kernel = new TFIDF(sentencesMap  /* query-relevant diversity */, true);
		
		ResultListSelector mmr = new MMR(sentencesMap, 
				0.5d /* lambda: 0d is all weight on query sim */, 
				TFIDF_kernel /* sim */,
				TFIDF_kernel /* div */ );
		//		BreakIterator breakIterator = BreakIterator.getSentenceInstance();
		//		breakIterator.setText(text);
		//
		//		int start = breakIterator.first();
		//		for (int end = breakIterator.next();
		//				end != BreakIterator.DONE;
		//				start = end, end = breakIterator.next()) 
		//		{
		//			String sentence = breakIterator.substring(start,end);
		//			sentences.add(sentence);
		//		}

		for (String doc : sentencesMap.keySet())
			mmr.addDoc(doc);
		
		top5Sentences = mmr.getResultList(queryKeywords,30);
		return top5Sentences;
	}

}
