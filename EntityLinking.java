package example;


import java.io.*;
import java.util.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;

public class EntityLinking {
	
	public static String url = "http://localhost:2222/rest/annotate";
	
	private static ArrayList<String> getAnchors(String data) {
        ArrayList<String> entities = new ArrayList<String>();

        try {
            // Connect to database, retrieve entity-linked urls
            Document doc = Jsoup.connect(url)
                    .data("text", data)
                    .post();
            Elements links = doc.select("a[href]");

            // Parse urls, returning only the last word of the url (after the last /)
            for (Element e : links) {
                String title = e.attr("title");
                title = title.substring(title.lastIndexOf("/") + 1);
                entities.add(title);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return entities;
    }

	public static void main(String[] args) throws IOException {
		
		String fileName = "dedup.articles-paragraphs.cbor"; //args[0]
		String outputPath = "C:\\Users\\ddash\\Documents\\Spring2018\\Data_Science\\DSoutput"; //args[1]
		
		InputStream is = new FileInputStream(fileName);
		BufferedInputStream bis = new BufferedInputStream(is);
		
		Iterator<Data.Paragraph> ip = DeserializeData.iterParagraphs(bis);
		
		File paraText = new File(outputPath + "/paraID");
		paraText.createNewFile();
		FileWriter writer = new FileWriter(paraText);
			
		while(ip.hasNext()) {
		    writer.write(ip.next().getParaId());
		    writer.write(" --> ");
		    String paraContent = ip.next().getTextOnly();
		    ArrayList<String> entities = getAnchors(paraContent);
		    for(String s: entities) {
		    	writer.write(s);
		    	writer.write(", ");
		    }
		    writer.write(System.getProperty( "line.separator" ));
		}
		    
		writer.flush();
		writer.close();

	}

}

