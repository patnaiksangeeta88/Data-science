package lucene.dbpedia;

import static org.hamcrest.CoreMatchers.instanceOf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Index {
	
	public static HashMap<String, Integer> hm = new HashMap<>();
	public static HashMap<String, String> cat = new HashMap<>();
	
	public static void main(String[] args) throws IOException {
		String folderLocation = "C:\\Users\\Debajyoti\\Downloads\\curloutput";
		File folder = new File(folderLocation);
		
		File[] lisOfFiles = folder.listFiles();
		for(File f: lisOfFiles) {
			if(f.isFile()) {
				readFile(f);
			}
		}
	
		String fileName = "C:\\Users\\Debajyoti\\Downloads\\curloutput\\entity.txt";
		FileWriter fw = new FileWriter(fileName);
		
		StringBuilder builder = new StringBuilder();
		String line = null;
		if(hm.size()>0) {
			for(Map.Entry<String, Integer> e: hm.entrySet()) {
				line = e.getKey() +"     "+e.getValue();
				builder.append(line);
                builder.append(System.getProperty("line.separator"));
			}
		}
		fw.write(builder.toString());
		fw.close();
		
		String fileName1 = "C:\\Users\\Debajyoti\\Downloads\\curloutput\\typemap.txt";
		FileWriter fw1 = new FileWriter(fileName1);
		
		StringBuilder builder1 = new StringBuilder();
		String line1 = null;
		if(cat.size()>0) {
			for(Map.Entry<String, String> e: cat.entrySet()) {
				line1 = e.getKey() +"     "+e.getValue();
				builder1.append(line1);
                builder1.append(System.getProperty("line.separator"));
			}
		}
		fw1.write(builder1.toString());
		fw1.close();

	}
	
	public static void readFile(File fileName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));

        try {
            System.out.println("Now reading file ["+ fileName + "]");
            
            String line = br.readLine();         
            while (line != null) {
            	int i = line.indexOf("types=");
            	int j = line.indexOf("\" surfaceForm=");
            	int k = line.indexOf("\" offset=");
            	if(i>0 && j>0) {
            		//System.out.println(line.substring(i+7, j));
            		//String[] types = line.substring(i+7, j).split(",");
            		String types = line.substring(i+7, j);
            		String entity = line.substring(j+15, k);
            		indexAncCount(types, entity);
            	}
            	
                line = br.readLine();
            }
            
        } finally {
            br.close();
        }
	}
	/*
	public static void indexAncCount(String[] types) {
		for(String s: types) {
			if(s.length()>1) {
				if(hm.containsKey(s)) {
					int val = hm.get(s);
					hm.put(s, val+1);
				}
				else
					hm.put(s, 1);
			}
		}
	}
	*/
	
	public static void indexAncCount(String t, String e) {
		if(e.length()>1) {
			if(hm.containsKey(e)) {
				int val = hm.get(e);
				hm.put(e, val+1);
			}
			else
				hm.put(e, 1);
			
			if(!cat.containsKey(e)) {
				cat.put(e, t);
			}
			
		}
		
	}

}
