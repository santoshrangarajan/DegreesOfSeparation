package com.abstractlayers.degrees.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.abstractlayers.degrees.ds.Node;

public class FileUtils {
	public void readFile ( String fileName){
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(fileName));
			while ((sCurrentLine = br.readLine()) != null) {
				System.out.println(sCurrentLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public List<String> getLinesFromFile(String fileName) throws Exception{
		List<String> lines = new ArrayList<String>();
		BufferedReader br = null;
	    String sCurrentLine;
		br = new BufferedReader(new FileReader(fileName));
		while ((sCurrentLine = br.readLine()) != null) {
				lines.add(sCurrentLine);
		 }
		if (br != null)br.close();
		return lines;
	}
	
	public void writeContentToFile(String content , String directory, String fileName) throws IOException{
		 File file = new File(directory+fileName);
		  if (!file.exists()) {
				file.createNewFile();
			}
		  
		  FileWriter fw = new FileWriter(file.getAbsoluteFile());
		  BufferedWriter bw = new BufferedWriter(fw);
		  bw.write(content.toString());
		  bw.close();
	}
	
	
	public Map<Integer,Node > parseHadoopOutputFile(String fileName){
		BufferedReader br = null;
		Map<Integer,Node> nodeMap = new HashMap<Integer,Node>();
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(fileName));
			while ((sCurrentLine = br.readLine()) != null) {
				Node node = new Node(sCurrentLine);
				///// Only add nodes which are reachable.
				if(node.getDistance()!=Integer.MAX_VALUE) {
				    nodeMap.put(node.getId(),node);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return nodeMap;
	}
	
	
	/*
	 * for sourceVertex- set distance to 0
	 *                 - set color to gray
	 *                 
	 *                 
	 * Format of file
	 * 
	 * id  edges:distance:color:lastNodeReachedFrom
	 * 1   2,3,4:1:GRAY:1
	 */
	/*public void writeGraphToFile(Graph graph, String fileName, int sourceVertex) throws IOException{
		StringBuilder content= new StringBuilder();
		int distance=0;
		NodeColor color;
		//int edgeTo=0;
		
		  for(int v=0;v<graph.V();v++){
		    	Iterable<Integer> adjacentVertices = graph.adj(v);
		    	Iterator<Integer> vertexIterator = adjacentVertices.iterator();
		    	content.append(v+" ");
		    	String delim="";
		    	int lastNodeReachedFrom =0;
		    	while(vertexIterator.hasNext()){
		    		int vertex = vertexIterator.next();
		    		content.append(delim).append(vertex);
		    		delim =",";
		    	}
		    	if(v == sourceVertex){
		    	   distance = 0;
		    	   color = NodeColor.GRAY;
		    	   lastNodeReachedFrom = sourceVertex;
		    	  // edgeTo=0;
		    	} else {
		    		distance = Integer.MAX_VALUE;
		    		color = NodeColor.WHITE;
		    		lastNodeReachedFrom = -1;
		    		//edgeTo=-1;
		    	}
		    	content.append(":").append(distance).append(":").append(color).append(":").append(lastNodeReachedFrom).append(":");	
		    	content.append("\n");
		    }
		  
		  
		  File file = new File("/Users/santoshrangarajan/Development/Hadoop/"+fileName);
		  if (!file.exists()) {
				file.createNewFile();
			}
		  
		  FileWriter fw = new FileWriter(file.getAbsoluteFile());
		  BufferedWriter bw = new BufferedWriter(fw);
		  bw.write(content.toString());
		  bw.close();
	}*/
	
	
	//public void readFile
}
