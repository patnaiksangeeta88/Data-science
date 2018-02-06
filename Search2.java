package example;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.Data.PageSkeleton;
import edu.unh.cs.treccar_v2.Data.Section;
import edu.unh.cs.treccar_v2.read_data.CborFileTypeException;
import edu.unh.cs.treccar_v2.read_data.CborRuntimeException;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.FileWriter;

public class Search2 {
	
	//Author: Laura and Peihao
	public static void main(String[] args) throws IOException {
		
        System.setProperty("file.encoding", "UTF-8");
        
        String pagesFile = args[0];
        String indexPath = args[1];
        String outputPath = args[2];
        
        File runfile = new File(outputPath + "/runfile_page_0");
		runfile.createNewFile();
		FileWriter writer = new FileWriter(runfile);
        
        //paragraphs-run-sections
		IndexSearcher searcher = setupIndexSearcher(indexPath, "paragraph.lucene");
        searcher.setSimilarity(new BM25Similarity());
        final MyQueryBuilder queryBuilder = new MyQueryBuilder(new StandardAnalyzer());
        final FileInputStream fileInputStream3 = new FileInputStream(new File(pagesFile));
        
        System.out.println("starting searching for pages ...");
        
        int count = 0;
        Set<String> hs = new HashSet<>();
        for (Data.Page page : DeserializeData.iterableAnnotations(fileInputStream3)) {
        	
            //final String queryId = page.getPageName(); //New Changes -- Heading Weights for Page name
        	final String queryId = page.getPageId();//test

    
            String queryStr = buildSectionQueryStr(page, Collections.<Data.Section>emptyList());
            
            TopDocs tops = searcher.search(queryBuilder.toQuery(queryStr), 100);
            ScoreDoc[] scoreDoc = tops.scoreDocs;
            StringBuffer sb = new StringBuffer();
            float searchScore=0;
            int searchRank=0;
            for (int i = 0; i < scoreDoc.length; i++) {
            	
                ScoreDoc score = scoreDoc[i];
                final Document doc = searcher.doc(score.doc); // to access stored content
                // print score and internal docid
                //final String paragraphid = doc.getField("headingId").stringValue();
                
            	List<String> squeryId = new ArrayList<>();
            	squeryId.add(page.getSkeleton().toString()); //New Changes
                
                //Print the last heading of each page to file ***START***
        		int z = squeryId.toString().lastIndexOf("Section");
        		int x1 = squeryId.toString().indexOf("heading",z);
        		int y1 = squeryId.toString().lastIndexOf("headingId");
        		String ss=null;
                if(x1>=0 && y1>=0)
                	ss = squeryId.toString().substring(x1, y1);
                //***END***
                
                searchScore = score.score;
                searchRank = i+1;

                //writer.write(queryId+" Q0 "+paragraphid+" "+searchRank + " "+searchScore+" Lucene-BM25"); // For Page Wise Display (Assignment 1)
                System.out.println(".");
                if(!hs.contains(queryId+" Q0 "+ ss)) {
                	hs.add(queryId+" Q0 "+ ss);
                	writer.write(queryId+" Q0 "+ ss +" "+searchRank + " "+searchScore+" Lucene-BM25\n"); // Print the last heading of each page to file
                }
                
                count ++;
            }
            /*
            //print all concatenated headings to the file ***START***
            int x = squeryId.toString().indexOf("heading");
            int y = squeryId.toString().indexOf("headingId");
            sb.append(squeryId.toString().substring(x, y));
            while(squeryId.toString().indexOf("heading",y+7) > 0) {
            	x = squeryId.toString().indexOf("heading",y+7);
                y = squeryId.toString().indexOf("headingId",x);
                sb.append(squeryId.toString().substring(x, y));
            }
            writer.write(queryId+" Q0 "+ sb +" "+searchRank + " "+searchScore+" Lucene-BM25\n");
            // ***END***
             */
        }
        
        writer.flush();//why flush?
		writer.close();
		//stripDuplicatesFromFile(runfile.toString());
        System.out.println("Write " + count + " results\nQuery Done!");
        
	}
	
	//Author: Laura dietz
	static class MyQueryBuilder {

        private final StandardAnalyzer analyzer;
        private List<String> tokens;

        public MyQueryBuilder(StandardAnalyzer standardAnalyzer){
            analyzer = standardAnalyzer;
            tokens = new ArrayList<>(128);
        }

        public BooleanQuery toQuery(String queryStr) throws IOException {

            TokenStream tokenStream = analyzer.tokenStream("text", new StringReader(queryStr));
            tokenStream.reset();
            tokens.clear();
            while (tokenStream.incrementToken()) {
                final String token = tokenStream.getAttribute(CharTermAttribute.class).toString();
                tokens.add(token);
            }
            tokenStream.end();
            tokenStream.close();
            BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
            for (String token : tokens) {
                booleanQuery.add(new TermQuery(new Term("text", token)), BooleanClause.Occur.SHOULD);
            }
            return booleanQuery.build();
        }
    }
	
	//Author: Laura dietz
	private static IndexSearcher setupIndexSearcher(String indexPath, String typeIndex) throws IOException {
        Path path = FileSystems.getDefault().getPath(indexPath, typeIndex);
        Directory indexDir = FSDirectory.open(path);
        IndexReader reader = DirectoryReader.open(indexDir);
        return new IndexSearcher(reader);
    }
	
	//Author: Laura dietz
	private static String buildSectionQueryStr(Data.Page page, List<Data.Section> sectionPath) {
        StringBuilder queryStr = new StringBuilder();
        queryStr.append(page.getPageName());
        for (Data.Section section: sectionPath) {
            queryStr.append(" ").append(section.getHeading());
        }
        //System.out.println("queryStr = " + queryStr);
        return queryStr.toString();
    }
	
	public static void stripDuplicatesFromFile(String filename) throws IOException {
	    BufferedReader reader = new BufferedReader(new FileReader(filename));
	    Set<String> lines = new HashSet<String>(10000000); // maybe should be bigger
	    String line;
	    while ((line = reader.readLine()) != null) {
	        lines.add(line);
	    }
	    reader.close();
	    //System.out.println("Removing Duplicates");
	    BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
	    for (String unique : lines) {
	        writer.write(unique);
	        writer.newLine();
	    }
	    writer.close();
	}
	
	
}