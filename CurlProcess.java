package lucene.dbpedia;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.Data.Page.SectionPathParagraphs;
import edu.unh.cs.treccar_v2.Data.ParaBody;
import edu.unh.cs.treccar_v2.Data.Paragraph;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;

public class ReadAndProcessFile {
	
	public static String url = "http://localhost:2222/rest/annotate";
	public static String fileName = "C:\\Users\\Debajyoti\\Downloads\\curloutput\\c.txt";

	public static void main(String[] args) throws IOException {
		String folderLocation = "C:\\Users\\Debajyoti\\Downloads\\outfiles";
		File folder = new File(folderLocation);
		
		File[] lisOfFiles = folder.listFiles();
		for(File f: lisOfFiles) {
			if(f.isFile()) {
				readFile(f);
			}
		}
		

	}
	
	
	public static void readFile(File fileName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		
        try {
            System.out.println("Now reading file ["+ fileName + "]");
            
            String line = br.readLine();         
            while (line != null) {
            	String trimline = line.substring(45); // Remove the paraID from the starting of each line
            	processFile(trimline,url);
                line = br.readLine();
            }
            
        } finally {
            br.close();
        }
	}
	
	public static void processFile(String data, String url) throws IOException {
		
		String[] command = {"curl", url, "-H", "Accept: text/xml", "--data-urlencode","\"text="+data+"\"","--data","\"confidence=0.2\"","--data","\"support=20\""};
		ProcessBuilder process = new ProcessBuilder(command);
		Process p;
        FileWriter fw = new FileWriter(fileName, true);
        try
        {
        	p = process.start();
            BufferedReader reader =  new BufferedReader(new InputStreamReader(p.getInputStream()));   
            StringBuilder builder = new StringBuilder();

            String line = null;
            while ( (line = reader.readLine()) != null) {
            	builder.append(line);
                builder.append(System.getProperty("line.separator"));
                
            }
            //String result = builder.toString();
            fw.write(builder.toString());
            //System.out.println("done");

        }
        catch (IOException e)
        {   System.out.print("error");
            e.printStackTrace();
        }
        finally {
        	fw.close();
        }
		
	}
	


}
