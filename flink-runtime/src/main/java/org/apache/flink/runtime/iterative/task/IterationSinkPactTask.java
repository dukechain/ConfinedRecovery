/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.iterative.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.iterative.concurrent.SuperstepKickoffLatch;
import org.apache.flink.runtime.iterative.concurrent.SuperstepKickoffLatchBroker;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.operators.DataSinkTask;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.MutableObjectIterator;

import akka.pattern.Patterns;

public class IterationSinkPactTask<IT> extends DataSinkTask<IT> {

	private String brokerKey;
	
	private TaskConfig config;
	
	private volatile boolean terminationRequested;
	
	private int superstepNum = 1;
	
	private ArrayList<Path> writtenFiles = new ArrayList<Path>();
	
	FileOutputFormat<?> fileFormat = (FileOutputFormat<?>) format;
	
	private boolean isCheckpoint = false;
	
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
		
		// used for start from checkpoint during recovery
		if(this.config.getStartIteration() > 0) {
			this.superstepNum = this.config.getStartIteration();
		}
		
		SuperstepKickoffLatch nextSuperstepLatch = SuperstepKickoffLatchBroker.instance().get(brokerKey());
		
		// should always be the case. TODO enforce this
		if(format instanceof FileOutputFormat) {
			fileFormat = (FileOutputFormat<?>) format;
		}
		
		while(!taskCanceled && !terminationRequested()) {
				
			this.isCheckpoint = false;
			
			// delete old file if window is full
			if(writtenFiles.size() == fileFormat.getIterationWriteMode().getWriteWindow()) {
				fileFormat.getOutputFilePath().getFileSystem().delete(writtenFiles.remove(0), true);
			}
			
			// only write if writeInterval fits
			if(superstepNum % fileFormat.getIterationWriteMode().getWriteInterval() == 0) {
				
				// adjust path name with current superstep number
				String pathName = fileFormat.getOutputFilePath().toUri().toString();
				
				// append retry number to not overwrite existing checkpoint files
				if(this.config.getIterationRetry() > 0) {
					pathName += this.config.getIterationRetry();
				}
				
				if(this.superstepNum == fileFormat.getIterationWriteMode().getWriteInterval()) {
					pathName += "_"+this.superstepNum;
				}
				else {
					pathName = pathName.substring(0, pathName.length()-2)+"_"+this.superstepNum;
				}
				
				// set new path
				fileFormat.setOutputFilePath(new Path(pathName+"/"));
				
				System.out.println("Checkpointji "+pathName + this.getEnvironment().getIndexInSubtaskGroup()+ " / "+ this.getEnvironment().getNumberOfSubtasks());
			
				// do the write
				super.invoke();
				
				// keep track of written files
				writtenFiles.add(fileFormat.getOutputFilePath());
				
				if(pathName.contains("checkpoint")) {
					this.isCheckpoint = true;
				}
			
			}
			
			Environment env = getEnvironment();
			
			// Report end of superstep to JobManager
			TaskConfig taskConfig = new TaskConfig(getTaskConfiguration());
			JobManagerMessages.ReportIterationWorkerDone workerDoneEvent = new JobManagerMessages.ReportIterationWorkerDone(
					taskConfig.getIterationId(),
					new AccumulatorEvent(getEnvironment().getJobID(), new HashMap<String, Accumulator<?, ?>>()),
					this.isCheckpoint);

			
			Patterns.ask(env.getJobManager(), workerDoneEvent, 3600000);
			
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
