package com.abstractlayers.degrees.ds;

import java.util.*;


import org.apache.hadoop.io.Text;


public class Node {

  
  private final int id;
  private int distance;
  private List<Integer> edges = new ArrayList<Integer>();
  private NodeColor color = NodeColor.WHITE;
  private int lastNodeReachedFrom = 0; 
  

  public Node(String str) {

    String[] map = str.split("\\s+");
    String key = map[0]; ///// id
    String value = map[1].trim(); ///// edges

    String[] tokens = value.split(":");
    ////0-edges,1-distance,2-color,3-edgeto

    this.id = Integer.parseInt(key);

    ///// edges
    for (String s : tokens[0].split(",")) {
      if (s.length() > 0) {
        edges.add(Integer.parseInt(s));
      }
    }
    
    ////distance
    if (tokens[1].equals("Integer.MAX_VALUE")) {
      this.distance = Integer.MAX_VALUE;
    } else {
      this.distance = Integer.parseInt(tokens[1]);
    }
    
    ///color
    this.color = NodeColor.valueOf(tokens[2]);
    
    this.lastNodeReachedFrom = Integer.parseInt(tokens[3]);
    
    ///edgeTo
 /*   this.edgeTo = Integer.parseInt(tokens[3]);
    this.edgeTo = -1;*/

  }

  public int getLastNodeReachedFrom() {
	return lastNodeReachedFrom;
}

public void setLastNodeReachedFrom(int lastNodeReachedFrom) {
	this.lastNodeReachedFrom = lastNodeReachedFrom;
}

public Node(int id) {
    this.id = id;
  }

  public int getId() {
    return this.id;
  }

  public int getDistance() {
    return this.distance;
  }

  public void setDistance(int distance) {
    this.distance = distance;
  }

  public NodeColor getColor() {
    return this.color;
  }

  public void setColor(NodeColor color) {
    this.color = color;
  }

  public List<Integer> getEdges() {
    return this.edges;
  }

  public void setEdges(List<Integer> edges) {
    this.edges = edges;
  }
  
 /* public int getEdgeTo() {
		return edgeTo;
	}

	public void setEdgeTo(int edgeTo) {
		this.edgeTo = edgeTo;
	}*/

  public Text getLine() {
    StringBuffer s = new StringBuffer();
    
    /*for (int v : edges) {
      s.append(v).append(",");
    }*/
    int index=0;
    while(index < edges.size()){
    	if(index<edges.size()-1){
    		s.append(edges.get(index)).append(",");
    	} else {
    		s.append(edges.get(index));
    	}
    	index++;
    }
    //s.deleteCharAt(s.length());
    s.append(":");

    if (this.distance < Integer.MAX_VALUE) {
      s.append(this.distance).append(":");
    } else {
      s.append("Integer.MAX_VALUE").append(":");
    }

    s.append(color.toString()).append(":");
    
    s.append(lastNodeReachedFrom);
    

    return new Text(s.toString());
  }
  
  public static void main(String []argv){
	  String str="1 2,5:0:GRAY:5";
	  Node node = new Node(str);
	  
	  System.out.println("Id = "+node.getId());
	  System.out.println("line = "+node.getLine());
	  System.out.println("color="+ node.getColor());
	  System.out.println("lastNodeReachedFrom="+node.getLastNodeReachedFrom());
	  
  }

}