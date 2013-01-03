package com.abstractlayers.degrees.tasks;
        

import java.io.IOException;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.abstractlayers.degrees.ds.Node;
import com.abstractlayers.degrees.ds.NodeColor;

/*  Based of following
 * 
 *  Below are major changes
 *  1. updating lastNodeReachedFrom
 *  2. using context instead of output variable
 * */
public class BfsMapper extends Mapper<LongWritable, Text, IntWritable,Text>  {
	private Text word = new Text();
	private static final Log log = LogFactory.getLog(BfsMapper.class);
  

	 @Override
	 protected void map(LongWritable key, Text value, Context  context)  {
		  try{
		    
		    Node node = new Node(value.toString());
		    
	        // For each GRAY node, emit each of the edges as a new node (also GRAY)
	      if (node.getColor() == NodeColor.GRAY) {
	           for (int v : node.getEdges()) {
	           Node vnode = new Node(v);
	           vnode.setDistance(node.getDistance() + 1);
	           vnode.setLastNodeReachedFrom(node.getId());
	           vnode.setColor(NodeColor.GRAY);
			   context.write(new IntWritable(vnode.getId()), new Text(vnode.getLine()));
	        }
	        node.setColor(NodeColor.BLACK);
	      }
	    
	      // No matter what, we emit the input node
	      // If the node came into this method GRAY, it will be output as BLACK
	    
	      context.write(new IntWritable(node.getId()), new Text(node.getLine()));
	      //log.info("Write to reducer id ="+node.getId()+",Text="+text2.toString());
		  } catch(IOException ioex){
			  log.error("Key = "+key+",Value = "+value.toString(),ioex);
		  }catch(InterruptedException iex){
			  log.error("Key = "+key+",Value = "+value.toString(),iex);
		  }catch(Exception ex){
			  log.error("Key = "+key+",Value = "+value.toString(),ex);
		  }
		
	};

    }

    
	 
  



