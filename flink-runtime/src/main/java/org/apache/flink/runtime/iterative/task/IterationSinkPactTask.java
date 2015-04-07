package org.apache.flink.runtime.iterative.task;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.accumulators.AccumulatorEvent;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.iterative.concurrent.SuperstepKickoffLatch;
import org.apache.flink.runtime.iterative.concurrent.SuperstepKickoffLatchBroker;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.operators.DataSinkTask;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.core.fs.Path;

import akka.pattern.Patterns;
import scala.concurrent.Future;

public class IterationSinkPactTask<IT> extends DataSinkTask<IT> {

	private String brokerKey;
	
	private TaskConfig config;
	
	private volatile boolean terminationRequested;
	
	private int superstepNum = 1;
	
	/**
	 * The indices of the iterative inputs. Empty, if the task is not iterative. 
	 */
	protected int[] iterativeInputs;
	
	@Override
	public void invoke() throws Exception
	{
		
		// obtain task configuration (including stub parameters)
		Configuration taskConf = getTaskConfiguration();
		this.config = new TaskConfig(taskConf);
		
		// set the iterative status for inputs and broadcast inputs
		inputReader.setIterativeReader();
		
		keepFormatOpen = true;
		
		System.out.println("S "+brokerKey());
		SuperstepKickoffLatch nextSuperstepLatch = SuperstepKickoffLatchBroker.instance().get(brokerKey());
		
		while(!taskCanceled && !terminationRequested()) {
			
			if(format instanceof CsvOutputFormat) {
				CsvOutputFormat csv = (CsvOutputFormat) format;
				csv.setOutputFilePath(new Path(csv.getOutputFilePath().toUri().toString()+"_"+this.superstepNum));
			}
			
			super.invoke();
			
			// Report end of superstep to JobManager
			TaskConfig taskConfig = new TaskConfig(getTaskConfiguration());
			JobManagerMessages.ReportIterationWorkerDone workerDoneEvent = new JobManagerMessages.ReportIterationWorkerDone(
					taskConfig.getIterationId(),
					new AccumulatorEvent(getEnvironment().getJobID(),
							new HashMap<String, Accumulator<?, ?>>()));
			
			//Patterns.ask(getEnvironment().getJobManager(),
			//		workerDoneEvent, 3600000); // 1 hour
			
			// check if termination was requested
			verifyEndOfSuperstepState();
			
			boolean terminate = nextSuperstepLatch.awaitStartOfSuperstepOrTermination(currentIteration() + 1);
			if (terminate) {
				requestTermination();
			}
			else {
				incrementIterationCounter();
			}

		}
	}
	
	public String brokerKey() {
		if (brokerKey == null) {
			int iterationId =  config.getIterationId();
			brokerKey = getEnvironment().getJobID().toString() + '#' + iterationId + '#' +
					getEnvironment().getIndexInSubtaskGroup();
		}
		return brokerKey;
	}
	
	public boolean terminationRequested() {
		return this.terminationRequested;
	}
	
	public void requestTermination() {
		this.terminationRequested = true;
	}
	
	protected boolean inFirstIteration() {
		return this.superstepNum == 1;
	}

	protected int currentIteration() {
		return this.superstepNum;
	}
	
	protected void incrementIterationCounter() {
		this.superstepNum++;
	}
	
	protected void verifyEndOfSuperstepState() throws IOException {
		MutableReader<?> reader = this.inputReader;

		if (!reader.isFinished()) {
			if (reader.hasReachedEndOfSuperstep()) {
				reader.startNextSuperstep();
			}
			else {
				// need to read and drop all non-consumed data until we reach the end-of-superstep
				@SuppressWarnings("unchecked")
				MutableObjectIterator<Object> inIter = (MutableObjectIterator<Object>) this.reader;
				Object o = inputTypeSerializerFactory.getSerializer().createInstance();
				while ((o = inIter.next(o)) != null);
				
				if (!reader.isFinished()) {
					// also reset the end-of-superstep state
					reader.startNextSuperstep();
				}
			}
		}
	}
}
