package example;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ReadFile {
	static Map<String, Integer> outlinks = new HashMap<>();
	static Map<String, List<String>> citations = new HashMap<>();
	static Map<String, Double> pagerank = new HashMap<>();
	public static void main(String[] args) {

		try{
			List<String> lines=Files.readAllLines(Paths.get("C:\\Users\\ddash\\Documents\\Spring2018\\Data Science\\Graph walk\\graph.txt"), Charset.forName("UTF-8"));
			for(String line: lines){
				String[] pages = line.split("\t");
				outlinks.put(pages[0], pages.length-1);
				if(!citations.containsKey(pages[0]))
					citations.put(pages[0], new ArrayList<>());
				if(!pagerank.containsKey(pages[0]))
					pagerank.put(pages[0], 0.15);
				for(int i=1; i<pages.length;i++){
					if(!pagerank.containsKey(pages[i]))
						pagerank.put(pages[i], 0.15);
					if(!citations.containsKey(pages[i])){
						List<String> ls = new ArrayList<>();
						ls.add(pages[0]);
						citations.put(pages[i], ls);
					}
					else{
						List<String> ls1 = citations.get(pages[i]);
						ls1.add(pages[0]);
						citations.put(pages[i], ls1);
					}
				}
					
			}
		}catch(Exception e){
			System.out.println(e);
		}
		
		//for(Map.Entry<String, Integer> m: outlinks.entrySet()){
		//	System.out.printf("Key : %s and Value: %s %n", m.getKey(), m.getValue());
		//}
		
		for(Map.Entry<String, List<String>> m: citations.entrySet()){
			//System.out.printf("Key: %s and Value: %s %n",m.getKey(), m.getValue());
			double x = calcRank(m.getValue(), m.getKey());
			pagerank.put(m.getKey(), x);
			
		}
		
			
		
		for(Map.Entry<String, Double> m: pagerank.entrySet()){
			System.out.printf("Key %s Value %s %n",m.getKey(), m.getValue());
		}
		
		System.out.println("***Exiting***");
	}
	
	public static double calcRank(List<String> ls, String s){
		double val=pagerank.get(s);
		for(int i=0; i<ls.size();i++){
			if(outlinks.get(ls.get(i))>0)
				val += 0.85 * (pagerank.get(ls.get(i))/outlinks.get(ls.get(i)));
		}	
		return val;
	}

}