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


/*
 * Algorithm
 * 1. 
 * 2. Read file and Create symbolGraph
 * 3. Iterate through G objectof SymbolGraphs and write to file
          For specific source -
 *         set distance -0
 *         color as gray
 *      everything else
 *          distance = infinty
 *          color = white
 *   
 * 4. Run spring batch 
 *      a. copy the file using groovy script to hadoop
 *      b. Run Maps and Reducers for decided number of iterations
 *      c. In addition vertex and edges outputfile should contain below additional parameters
 *         a. is processed - by color  - available
 *         b. distance to -            - available
 *         c. marked                   - not available. Not needed. can use color?
 *         d. edgeto                   - not available.
 *             -1 for initialization
 *              0 for source
 *              and nodeId for any other node from which this node is reacheable
 *              
 *      d. groovy script to copy generated output 
 *              
 * DegreesOfSepereration   
 * 1. Parse final Output. Create G Object
 *      Parse vertex- , distanceto , edgeTo, marked, 
 *      
 * 2. when query say some person xyz
 *    calculare reverse index from ST
 *    query the G object for distance
 * 
 * 
 * */
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
