package com.abstractlayers.degrees.tasks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.item.ExecutionContext;



public class IterationDecider implements JobExecutionDecider, StepExecutionListener{

	private static final Log log = LogFactory.getLog(IterationDecider.class);
	
	private int iteration = 0;
	private int maxIterations = 1;
	
	

	public void setMaxIterations(int maxIterations) {
		this.maxIterations = maxIterations;
	}
	
	public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
		
		if (++iteration >= maxIterations) {
			log.info("Iteration ="+iteration+". Step completed");
			return new FlowExecutionStatus("COMPLETED");
		} else {
			 log.info("Iteration ="+iteration+".Continuing..."); 
			 return new FlowExecutionStatus("CONTINUE");
		}
		}
	
	public void beforeStep(StepExecution stepExecution) {
		String input,output;
		
		input = (iteration ==0)?("input_"+iteration) :("output_"+(iteration));
		output = "output_"+(iteration+1);
		
		log.info("Iteration ="+iteration+", input="+input+",output="+output); 
		stepExecution.getExecutionContext().put("mr.input", input);
		stepExecution.getExecutionContext().put("mr.output", output);
		
	}

	public ExitStatus afterStep(StepExecution stepExecution) {
		// TODO Auto-generated method stub
		return null;
	}

}
