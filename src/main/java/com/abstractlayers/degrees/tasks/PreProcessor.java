package com.abstractlayers.degrees.tasks;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import com.abstractlayers.degrees.ds.SymbolGraphBfs;
import com.abstractlayers.degrees.utils.FileUtils;


public class PreProcessor implements Tasklet {
	
	//private String originalFile ;
	private SymbolGraphBfs symbolGraphBfs;
	private FileUtils fileUtils;
	private static final Log log = LogFactory.getLog(PreProcessor.class);
	private String localFile;
	private String localDirectory;
	 
		  
	public PreProcessor(SymbolGraphBfs symbolGraphBfs, FileUtils fileUtils, String localDirectory, String localFile) throws Exception{
	 
		this.symbolGraphBfs = symbolGraphBfs;
		this.fileUtils = fileUtils;
		this.localDirectory = localDirectory;
		this.localFile = localFile;
	}
	
	
	private void generateHadoopInput(){
		try {	
			
			String nodeMapAsString = symbolGraphBfs.getNodeMapBeforeBfsAsString();
			//fileUtils.writeContentToFile(nodeMapAsString,localDirectory,"node_list_for_bfs_2.txt");
			fileUtils.writeContentToFile(nodeMapAsString,localDirectory,localFile);
		} catch (IOException e) {
			log.error("Exception writing SymbolGraph to file",e);
		}
	}
	
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		// TODO Auto-generated method stub
		log.info("Pre_processor execute called");
		generateHadoopInput();
		log.info("Created input file for hadoop");
		return RepeatStatus.FINISHED;
	}
	
	public static void main(String[] argv) throws Exception {
	
		SymbolGraphBfs symbolGraphBfs = new SymbolGraphBfs("/Users/santoshrangarajan/Development/Hadoop/movies.txt","/","Bacon, Kevin");
		FileUtils fileUtils = new FileUtils();	
		//PreProcessor preProcessor = new PreProcessor(symbolGraphBfs,fileUtils);
		//preProcessor.generateHadoopInput();
	   
	}

	

}
