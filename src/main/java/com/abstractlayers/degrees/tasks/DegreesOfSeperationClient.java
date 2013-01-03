package com.abstractlayers.degrees.tasks;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import com.abstractlayers.degrees.ds.SymbolGraphBfs;

/*
 * Sample client to verify  connections
 * 
 * */
public class DegreesOfSeperationClient implements Tasklet {
	
	
	 private SymbolGraphBfs symbolGraphBfs;
	 private String destination;
	 private static final Log log = LogFactory.getLog(DegreesOfSeperationClient.class);

	 public void setSymbolGraphBfs(SymbolGraphBfs symbolGraph) {
			this.symbolGraphBfs = symbolGraph;
	 }
	
	 public void setDestination(String destination){
		    this.destination = destination;
	 }
	 
	 public RepeatStatus execute(StepContribution contribution,ChunkContext chunkContext) throws Exception {
		if(symbolGraphBfs.isConnectionsSet()){
		 //log.info("Total connections for "+source+":"+symbolGraphBfs.getNodeMapAfterBfs().size());
			log.info("Source Name :"+symbolGraphBfs.getSourceName());
			log.info("Total connections :"+symbolGraphBfs.getConnectionsCount());
			log.info("First degree connections :"+symbolGraphBfs.getConnectionsCount(1));
			log.info("Second degree connections :"+symbolGraphBfs.getConnectionsCount(2));
			log.info("Third degree connections :"+symbolGraphBfs.getConnectionsCount(3));
			log.info("Distance between "+symbolGraphBfs.getSourceName()+" and "+ destination +":"+symbolGraphBfs.getConnectionDistance("Smith, Isaac"));
			log.info("Below is degree of seperation between "+symbolGraphBfs.getSourceName()+" and "+destination);
			List<String> connections = symbolGraphBfs.getConnectionDetails(destination);
			Iterator <String> connectionsIterator = connections.iterator();
			while(connectionsIterator.hasNext()){
				log.info(connectionsIterator.next());
			}
		} else {
		  log.info("Connections not initialized yet");
	   }
		return RepeatStatus.FINISHED;
	}
	
	
	public static void main(String []argv){
	      DegreesOfSeperationClient dos = new DegreesOfSeperationClient();     
	}

}
