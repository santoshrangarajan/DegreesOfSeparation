package com.abstractlayers.degrees.tasks;




import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import com.abstractlayers.degrees.ds.SymbolGraphBfs;


public class PostProcessor implements Tasklet{
	
	private String localDirectory;
	private String localFile;
	private SymbolGraphBfs symbolGraphBfs;
	private static final Log log = LogFactory.getLog(PostProcessor.class);
	
	
	public PostProcessor(SymbolGraphBfs symbolGraphBfs, String localDirectory, String localFile){
		this.symbolGraphBfs = symbolGraphBfs;
		//this.hadoopProcessedFile = hadoopProcessedFile;
		this.localDirectory=localDirectory;
		this.localFile=localFile;
	}
	
	 private void processHadoopOutput(){
	    	symbolGraphBfs.initConnectionsMap(localDirectory+localFile);
	    }

      public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
			
			log.info("Initializing connections Map");
			processHadoopOutput();
			log.info("Connections map initialized successfully");
			return RepeatStatus.FINISHED;	
		}
	
}
