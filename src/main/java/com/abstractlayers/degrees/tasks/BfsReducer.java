package com.abstractlayers.degrees.tasks;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import com.abstractlayers.degrees.ds.Node;
import com.abstractlayers.degrees.ds.NodeColor;


/*  Based of johnandcallin.com blog
 *  Below are major changes
 *  1. updating lastNodeReachedFrom
 * */
public class BfsReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

	private static final Log log = LogFactory.getLog(BfsReducer.class);
	
	   @Override
	    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		 
		 try{
		   List<Integer> edges = null;
	       int distance = Integer.MAX_VALUE;
	       NodeColor color = NodeColor.WHITE;
           int lastNodeReachedFrom = -1;
	       Iterator<Text> itr = values.iterator();
	        while (itr.hasNext()) {
	        	
	        Text value = itr.next();
	        try{
		        Node u = new Node(key.get()+" "+value.toString());
		        // One (and only one) copy of the node will be the fully expanded
		        // version, which includes the edges
		        if (u.getEdges().size() > 0) {
		          edges = u.getEdges();
		        }
	
		        // Save the minimum distance
		        if (u.getDistance() < distance) {
		          distance = u.getDistance();
		          ///// update lastNodeReachedFrom associated with minimum distance
		          lastNodeReachedFrom = u.getLastNodeReachedFrom();
		        }
	
		        // Save the darkest color
		        if (u.getColor().ordinal() > color.ordinal()) {
		          color = u.getColor();
		        }
	        } catch(Exception ex){
	        	 log.error("Key = "+key+",Value = "+value.toString(),ex);
	        }
	       
	      }

	      Node n = new Node(key.get());
	      n.setDistance(distance);
	      n.setEdges(edges);
	      n.setColor(color);
	      n.setLastNodeReachedFrom(lastNodeReachedFrom);
	     
	      log.info("Node id ="+n.getId()+"color = "+n.getColor());
	      Text text = new Text(n.getLine());
	      context.write(key, text);
	    
		 } catch(IOException ioex){
			 log.error("Key = "+key,ioex);
		 } catch(InterruptedException iex){
			 log.error("Key = "+key,iex);
		 } catch(Exception ex){
			 log.error("Key = "+key,ex);
		 }
	    	
	    }
	  
	
}
