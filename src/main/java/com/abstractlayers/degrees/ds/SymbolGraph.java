package com.abstractlayers.degrees.ds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.abstractlayers.degrees.tasks.PreProcessor;
import com.abstractlayers.degrees.utils.FileUtils;

public class SymbolGraph {
	
	protected HashMap<String,Integer> symbolTable; //// Map of keywords and index in file
	protected TreeMap<Integer,List<Integer>> adjList; ///// adjacencyList
	protected FileUtils fileUtils;
	private String delimiter;
	protected String[] invertedIndex;           // index  -> string
	private static final Log log = LogFactory.getLog(SymbolGraph.class);
	
	
    public SymbolGraph(String fileName, String delimiter){
    try{
    	log.info("Initializing SymbolGraph for file="+fileName+", delimiter="+delimiter);
    	this.delimiter = delimiter;
    	fileUtils = new FileUtils();
    	List<String> lines = fileUtils.getLinesFromFile(fileName);
    	symbolTable = new HashMap<String,Integer>();
    	populateSymbolTable(lines);
    	initInvertedIndex();
    	initAdjList();
    	populateAdjList(lines);
    	log.info("SymbolGrap initialized");
     } catch (Exception ex){
    	log.error("Exception creating SymbolGraph",ex);
     }
    	
    }
    
    public SymbolGraph(String delimiter){
    	this.delimiter = delimiter;
    	symbolTable = new HashMap<String,Integer>();
    	initAdjList();
    }
    
    public void populateSymbolTable(String line){
    	log.info("Populating symbolTable...");
    	 String[] tokens = line.split(delimiter);
         for (int i = 0; i < tokens.length; i++) {
             if (!symbolTable.containsKey(tokens[i]))
             	symbolTable.put(tokens[i], symbolTable.size());
         }
    }
    public void populateSymbolTable(List<String> lines){
    	
    	for(String line:lines){
    		 String[] tokens = line.split(delimiter);
	            for (int i = 0; i < tokens.length; i++) {
	                if (!symbolTable.containsKey(tokens[i]))
	                	symbolTable.put(tokens[i], symbolTable.size());
	            }
    	}
    }
    
   public void initAdjList(){
    	adjList = new TreeMap<Integer,List<Integer>>();
    	 for (int i = 0; i < symbolTable.size(); i++) {
    		 adjList.put(i,new ArrayList<Integer>());
	        }
    }
    
    private void initInvertedIndex(){
    	invertedIndex = new String[symbolTable.size()];
	        for (String name : symbolTable.keySet()) {
	        	invertedIndex[symbolTable.get(name)] = name;
	        }
    }
    
    public void populateAdjList(List<String> lines){
    	for(String line:lines){ 
	        	//System.out.println("second loop- count"+count);
	            String[] tokens = line.split(delimiter);
	            int v = symbolTable.get(tokens[0]);
	            for (int i = 1; i < tokens.length; i++) {
	                int w = symbolTable.get(tokens[i]);    
	                List<Integer>  nodeListForVertexV = adjList.get(v);
	                nodeListForVertexV.add(w);
	                List<Integer>  nodeListForVertexW = adjList.get(w);
	                nodeListForVertexW.add(v);      
	            }
    	}
    }
    
   
    
    
    public int index(String s) {
        return symbolTable.get(s);
    }
    
    public String name(int i) {
        return invertedIndex[i];
    }
    
    /* Adj List in form of String */
    public String getAdjListAsString() {
		//return adjList;
    	StringBuilder content= new StringBuilder();
    	Iterator<Integer> adjKeysIterator = adjList.keySet().iterator();
    	
    	while(adjKeysIterator.hasNext()){
    		int key = adjKeysIterator.next();
    		content.append(key);
    		content.append(" ");
    		List<Integer> list = adjList.get(key);
    		String delim="";
    		for(int i : list){
    			content.append(delim).append(i);
	    		delim =",";
    		}
    		content.append("\n");
    	}
    	return content.toString();
	}
    
    
    
    public static void main(String[] argv) {
    	
    	try{
    	  String filename  = "/Users/santoshrangarajan/Development/JavaWS/Algorithms/movies.txt";
	      String delimiter = "/";
	      SymbolGraph sg = new SymbolGraph(filename, delimiter);
	      String source = "Bacon, Kevin";
          int s = sg.index(source);
          FileUtils fileUtils = new FileUtils();
         // fileUtils.writeContentToFile(sg.getAdjListAsString(), "sg_adj_list.txt");
          System.out.println("Written adjacency list to file");
    	} catch(Exception ex){
    		ex.printStackTrace();
    	}
    }
}
