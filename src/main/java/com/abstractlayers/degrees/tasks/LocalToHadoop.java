package com.abstractlayers.degrees.tasks;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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


public class LocalToHadoop implements Tasklet {

	private static final Log log = LogFactory.getLog(LocalToHadoop.class);
	
	 private String localFile;
	 private String localDirectory;
	 private String hadoopFile;
	 private String hadoopDirectory;
	 private String confCoreSite;
	 private String confHdfsSite;
	 private Configuration hadoopConf;
	 
	 public void setLocalFile(String localFile) {
			this.localFile = localFile;
		}

		public void setLocalDirectory(String localDirectory) {
			this.localDirectory = localDirectory;
		}

		public void setHadoopFile(String hadoopFile) {
			this.hadoopFile = hadoopFile;
		}

		public void setHadoopDirectory(String hadoopDirectory) {
			this.hadoopDirectory = hadoopDirectory;
		}

		public void setConfCoreSite(String confCoreSite) {
			this.confCoreSite = confCoreSite;
		}

		public void setConfHdfsSite(String confHdfsSite) {
			this.confHdfsSite = confHdfsSite;
		}

		

		public void setHadoopConf(Configuration hadoopConf) {
			this.hadoopConf = hadoopConf;
		}
	
	/*Assumptions
	 * all output directories needs to be deleted
	 * input_0 needs to be created
	 * */
	public RepeatStatus execute(StepContribution contribution,	ChunkContext chunkContext) throws Exception {
		
		log.info("Copying local file to Hadoop");
		hadoopConf.addResource(new Path(confCoreSite));
	    hadoopConf.addResource(new Path(confHdfsSite));
	    Path destPath = new Path(hadoopDirectory+hadoopFile);
	    Path srcPath = new Path(localDirectory+localFile);
	    FileSystem.get(hadoopConf).copyFromLocalFile(srcPath,destPath);
	    log.info("Copied file successfully");
	
	   return RepeatStatus.FINISHED;
	}
	
    public static void main(String [] argv){
    	LocalToHadoop localToHadoop = new LocalToHadoop();
    	//copyTask.copyFileToHadoop();
    }

}
