package com.abstractlayers.degrees.tasks;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class HadoopToLocal implements Tasklet{
	
private static final Log log = LogFactory.getLog(LocalToHadoop.class);
	
	private String hadoopFile;
	private String hadoopDirectory;
	private String localFile;
	private String localDirectory;
	private Configuration hadoopConf;
	private String confCoreSite;
	private String confHdfsSite;
	
	

	public void setConfCoreSite(String confCoreSite) {
		this.confCoreSite = confCoreSite;
	}

	public void setConfHdfsSite(String confHdfsSite) {
		this.confHdfsSite = confHdfsSite;
	}

	public void setHadoopConf(Configuration conf){
		this.hadoopConf =  conf;
	}
	
	public void setHadoopFile(String hadoopFile) {
		this.hadoopFile = hadoopFile;
	}


	public void setHadoopDirectory(String hadoopDirectory) {
		this.hadoopDirectory = hadoopDirectory;
	}


	public void setLocalFile(String localFile) {
		this.localFile = localFile;
	}


	public void setLocalDirectory(String localDirectory) {
		this.localDirectory = localDirectory;
	}




	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		log.info("Copying hadoop file to local filesystem");
		 hadoopConf.addResource(new Path(confCoreSite));
	     hadoopConf.addResource(new Path(confHdfsSite));
	        
	    Path srcPath = new Path(hadoopDirectory+hadoopFile);
	    Path destPath = new Path(localDirectory+localFile);
	    FileSystem.get(hadoopConf).copyToLocalFile(srcPath, destPath);
	    log.info("Copied file successfully");
	    return RepeatStatus.FINISHED;
	}

}
