package com.abstractlayers.degrees;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class DegreesOfSeparation {
	
	 private static final Log log = LogFactory.getLog(DegreesOfSeparation.class);
	 
	  public static void main(String[] arguments) throws Exception {
	    	AbstractApplicationContext ctx = new ClassPathXmlApplicationContext("/META-INF/spring/launch-context.xml",
	    			DegreesOfSeparation.class);
	    	
	    	JobLauncher jobLauncher = (JobLauncher) ctx.getBean(JobLauncher.class);
	    	Job job = ctx.getBean(Job.class);
	    	log.info("Starting degrees-of-separation");
    	    jobLauncher.run(job, new JobParametersBuilder().toJobParameters());
    	    log.info("Completed degrees-of-separation");                  
    	    
	    }
}
