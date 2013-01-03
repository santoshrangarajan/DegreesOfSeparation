package com.abstractlayers.degrees.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.abstractlayers.degrees.tasks.PreProcessor;
import com.abstractlayers.degrees.utils.FileUtils;

public class SymbolGraphBfs extends SymbolGraph {
	
	
	private TreeMap<Integer,Node> nodeMapBeforeBfs; //// map to store Id and Node values before BFS
	private TreeMap<Integer,Node> nodeConnectionsMap;  //// map to store node and its connections after bfs is completed
	
	private static final Log log = LogFactory.getLog(SymbolGraphBfs.class);
	///// DISTANCE, COLOR, LAST_NODE_REACHEDFROM
	public static final int DEFAULT_DISTANCE = Integer.MAX_VALUE;
	public static final NodeColor DEFAULT_COLOR = NodeColor.WHITE;
	public static final int DEFAULT_LAST_NODE_REACHED_FROM = -1;
	
	
	public static final int BFS_SOURCE_DISTANCE =0;
	public static final NodeColor BFS_SOURCE_COLOR = NodeColor.GRAY;
	
	private boolean connectionsSet;
	private String sourceName;
	

	/*
	 * Constructor invoked by pre-processor before BFS
	 * */
    public SymbolGraphBfs(String fileName, String delimiter,String sourceName){
   
    	super(fileName,delimiter);
    	nodeMapBeforeBfs = new TreeMap<Integer,Node>();
    	nodeConnectionsMap = new TreeMap<Integer,Node>();
    	this.sourceName = sourceName;
    	initNodeMapBeforeBfs(sourceName);
    	this.connectionsSet = false;
    }
     
    
    ///// key  Node
    //// Node -  edges : distance : color : lastNodeReachedFrom
    private void initNodeMapBeforeBfs(String sourceName){
    		
    	int source = symbolTable.get(sourceName);
    	Iterator<Integer> adjKeysIterator = adjList.keySet().iterator();
    	StringBuilder content = new StringBuilder();
    	int count =0;
    	while(adjKeysIterator.hasNext()) {
    		count++;
    		int key = adjKeysIterator.next();
    		content.append(key);
    		content.append(" ");
    		content.append(appendEdges(key));
    		content.append(appendBfsAttributes(key,source));
		   nodeMapBeforeBfs.put(key, new Node(content.toString()));
		   content.delete(0, content.length());
    	}
    }
    
    private String appendEdges( int key){
    	StringBuilder edges = new StringBuilder();
    	List<Integer> list = adjList.get(key);
		String delim="";
		for(int i : list){
			edges.append(delim).append(i);
    		delim =",";
		}
		//System.out.println("Edges ="+edges.toString());
		return edges.toString();
    }
    
    private String appendBfsAttributes(int key, int source){
    	StringBuilder attrs = new StringBuilder();
    	int distance =0;
    	NodeColor color;
    	int lastNodeReachedFrom;
    	
    	if(key == source){
	    	   distance = BFS_SOURCE_DISTANCE;
	    	   color = BFS_SOURCE_COLOR;
	    	   lastNodeReachedFrom = source;
	   } else {
	    		distance = DEFAULT_DISTANCE;
	    		color = DEFAULT_COLOR;
	    		lastNodeReachedFrom = DEFAULT_LAST_NODE_REACHED_FROM;
	   }
    	attrs.append(":").append(distance).append(":").append(color).append(":").append(lastNodeReachedFrom).append(":");
    	//System.out.println("Attributes ="+attrs.toString());
    	return attrs.toString();
    }
    
    public String getNodeMapBeforeBfsAsString(){
    	StringBuilder content= new StringBuilder();
    	Iterator<Integer> nodeMapIterator = nodeMapBeforeBfs.keySet().iterator();	
    	while(nodeMapIterator.hasNext()){
    		int key = nodeMapIterator.next();
    		Node node= nodeMapBeforeBfs.get(key);
    		content.append(key+" "+node.getLine()+"\n");
    	}
    	return content.toString();
    }

   /* private void updateConnectionsMap(List<String> lines){
    	for(String line : lines){
    		Node node = new Node(line);
			///// Only add nodes which are reachable.
			if(node.getDistance()!=Integer.MAX_VALUE) {
				nodeConnectionsMap.put(node.getId(),node);
			}
    	}
    }*/
    
    public TreeMap<Integer,Node> getNodeMapAfterBfs(){
    	return nodeConnectionsMap;
    }
    
    public void initConnectionsMap(String hadoopOutputFile) {
    	try {
    	List<String> lines = fileUtils.getLinesFromFile(hadoopOutputFile);
    	for(String line : lines){
    		Node node = new Node(line);
			///// Only add nodes which are reachable.
			if(node.getDistance()!=Integer.MAX_VALUE) {
				nodeConnectionsMap.put(node.getId(),node);
			}
    	}
    	connectionsSet = true;
    	} catch(Exception ex){
    		log.error("Exception creating connectionsMap from Hadoop file",ex);
    	}
    }
    
    public boolean isConnectionsSet() {
		return connectionsSet;
	}
    /*
     *  Below methods for client interaction
     *  
     * */
    
    public String getSourceName(){
    	return sourceName;
    }
    
    /* Returns all connections count */
    public int getConnectionsCount(){
    	return nodeConnectionsMap.size();
    }
    
    /*Returns connection count for degree passed
     *Can be cached. No need to compute every time
     */
    public int getConnectionsCount(int degree){
    	 int count = 0;
    	 Iterator<Integer> keysIterator= nodeConnectionsMap.keySet().iterator();
    	 while(keysIterator.hasNext()){
 	    	int key = keysIterator.next();
 	    	Node node = nodeConnectionsMap.get(key);
 	    	if(node.getDistance()<=degree) {
 	    		count++;
 	    	}
 	    }
    	return count;
    }
    
    
    public int getConnectionDistance(String name){
	   Node node = nodeConnectionsMap.get(index(name));
	   return node.getDistance();
	}
    public List<String> getConnectionDetails(String name){
    	List<String> connections = new ArrayList<String>();
    	int vertex = index(name);
		int sourceVertex = index(sourceName);
		
		//log.info("Depth of connection ="+nodeMap.get(vertex).getDistance());
		
	    Stack<Integer> path = new Stack<Integer>();
	    int lastNodeReachedFrom = vertex;
	    path.push(vertex);
	    while(lastNodeReachedFrom != sourceVertex){
	    	Node node = nodeConnectionsMap.get(lastNodeReachedFrom);
	    	lastNodeReachedFrom = node.getLastNodeReachedFrom();
	    	path.push(lastNodeReachedFrom);
	    }
	    while(!path.isEmpty()){
	    	connections.add(name(path.pop()));
	    } 
	    return connections;
    }
    
    public static void main(String[] argv){
    	try{
    	  String filename  = "/Users/santoshrangarajan/Development/JavaWS/Algorithms/movies.txt";
	      String delimiter = "/";
	      SymbolGraphBfs sg = new SymbolGraphBfs(filename, delimiter,"Bacon, Kevin");
	      FileUtils fileUtils = new FileUtils();
        //  fileUtils.writeContentToFile(sg.getNodeMapBeforeBfsAsString(), "sg_node_list.txt");
          System.out.println("Written Nodes to file");
    	} catch(Exception ex){
    		ex.printStackTrace();
    	}
    }
}
