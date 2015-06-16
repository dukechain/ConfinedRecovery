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
import java.io.Serializable;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.operators.util.JoinHashMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.iterative.concurrent.BlockingBackChannel;
import org.apache.flink.runtime.iterative.concurrent.BlockingBackChannelBroker;
import org.apache.flink.runtime.iterative.concurrent.Broker;
import org.apache.flink.runtime.iterative.concurrent.IterationAccumulatorBroker;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetBroker;
import org.apache.flink.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion;
import org.apache.flink.runtime.iterative.io.SolutionSetObjectsUpdateOutputCollector;
import org.apache.flink.runtime.iterative.io.SolutionSetUpdateOutputCollector;
import org.apache.flink.runtime.iterative.io.WorksetUpdateOutputCollector;
import org.apache.flink.runtime.operators.PactDriver;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.operators.ResettablePactDriver;
import org.apache.flink.runtime.operators.hash.CompactingHashTable;
import org.apache.flink.runtime.operators.util.DistributedRuntimeUDFContext;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The abstract base class for all tasks able to participate in an iteration.
 */
public abstract class AbstractIterativePactTask<S extends Function, OT> extends RegularPactTask<S, OT>
		implements Terminable
{
	private static final Logger log = LoggerFactory.getLogger(AbstractIterativePactTask.class);
	
	protected LongCounter worksetAccumulator;

	protected BlockingBackChannel worksetBackChannel;

	protected boolean isWorksetIteration;

	protected boolean isWorksetUpdate;

	protected boolean isSolutionSetUpdate;

	private RuntimeAccumulatorRegistry iterationAccumulators;

	private String brokerKey;

	private int superstepNum = 1;
	
	private volatile boolean terminationRequested;
	
	protected int feedbackDataInput; 

	// --------------------------------------------------------------------------------------------
	// Main life cycle methods that implement the iterative behavior
	// --------------------------------------------------------------------------------------------

	@Override
	protected void initialize() throws Exception {
		super.initialize();

		// check if the driver is resettable
		if (this.driver instanceof ResettablePactDriver) {
			final ResettablePactDriver<?, ?> resDriver = (ResettablePactDriver<?, ?>) this.driver;
			// make sure that the according inputs are not reseted
			for (int i = 0; i < resDriver.getNumberOfInputs(); i++) {
				if (resDriver.isInputResettable(i)) {
					excludeFromReset(i);
				}
			}
		}
		
		TaskConfig config = getLastTasksConfig();
		isWorksetIteration = config.getIsWorksetIteration();
		isWorksetUpdate = config.getIsWorksetUpdate();
		isSolutionSetUpdate = config.getIsSolutionSetUpdate();

		if (isWorksetUpdate) {
			worksetBackChannel = BlockingBackChannelBroker.instance().getAndRemove(brokerKey());

			if (isWorksetIteration) {
				worksetAccumulator = getIterationAccumulators().getAccumulator(WorksetEmptyConvergenceCriterion.ACCUMULATOR_NAME);
				
				if (worksetAccumulator == null) {
					throw new RuntimeException("Missing workset elements count accumulator.");
				}
			}
		}
		
		// used for start from checkpoint during recovery
		if(this.config.getStartIteration() > 0) {
			this.superstepNum = this.config.getStartIteration();
		}
	}

	@Override
	public void run() throws Exception {
		
		if (inFirstIteration()) {
			if (this.driver instanceof ResettablePactDriver) {
				// initialize the repeatable driver
				((ResettablePactDriver<?, ?>) this.driver).initialize();
			}
		} else {
			reinstantiateDriver();
			resetAllInputs();
			
			// re-read the iterative broadcast variables
			for (int i : this.iterativeBroadcastInputs) {
				final String name = getTaskConfig().getBroadcastInputName(i);
				readAndSetBroadcastInput(i, name, this.runtimeUdfContext, superstepNum, true);
			}
			
			// reinit local strategies for refined recovery
//			if(this.config.getRefinedRecoveryEnd() + 1 == currentIteration()) {
//				initInputLocalStrategy(feedbackDataInput);
//			}
		}
		
		
		// infuse records during refined recovery?
		infuseRecoveryRecords();

		// call the parent to execute the superstep
		super.run();
		
		// release the iterative broadcast variables
		for (int i : this.iterativeBroadcastInputs) {
			final String name = getTaskConfig().getBroadcastInputName(i);
			//releaseBroadcastVariables(name, superstepNum, this.runtimeUdfContext);
		}
	}

	@Override
	protected void closeLocalStrategiesAndCaches() {
		try {
			super.closeLocalStrategiesAndCaches();
		}
		finally {
			if (this.driver instanceof ResettablePactDriver) {
				final ResettablePactDriver<?, ?> resDriver = (ResettablePactDriver<?, ?>) this.driver;
				try {
					resDriver.teardown();
				} catch (Throwable t) {
					log.error("Error while shutting down an iterative operator.", t);
				}
			}
		}
	}

	@Override
	public DistributedRuntimeUDFContext createRuntimeContext(String taskName) {
		Environment env = getEnvironment();
		return new IterativeRuntimeUdfContext(taskName, env.getNumberOfSubtasks(),
				env.getIndexInSubtaskGroup(), getUserCodeClassLoader(), getExecutionConfig());
	}

	// --------------------------------------------------------------------------------------------
	// Utility Methods for Iteration Handling
	// --------------------------------------------------------------------------------------------

	protected boolean inFirstIteration() {
		return this.superstepNum == 1 || this.superstepNum == this.config.getStartIteration();
	}

	protected int currentIteration() {
		return this.superstepNum;
	}

	protected void incrementIterationCounter() {
		this.superstepNum++;
	}

	public String brokerKey() {
		if (brokerKey == null) {
			int iterationId = config.getIterationId();
			brokerKey = getEnvironment().getJobID().toString() + '#' + iterationId + '#' +
					getEnvironment().getIndexInSubtaskGroup();
		}
		return brokerKey;
	}

	private void reinstantiateDriver() throws Exception {
		if (this.driver instanceof ResettablePactDriver) {
			final ResettablePactDriver<?, ?> resDriver = (ResettablePactDriver<?, ?>) this.driver;
			resDriver.reset();
		} else {
			Class<? extends PactDriver<S, OT>> driverClass = this.config.getDriver();
			this.driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);

			try {
				this.driver.setup(this);
			}
			catch (Throwable t) {
				throw new Exception("The pact driver setup for '" + this.getEnvironment().getTaskName() +
						"' , caused an error: " + t.getMessage(), t);
			}
		}
	}


	public RuntimeAccumulatorRegistry getIterationAccumulators() {
		if (this.iterationAccumulators == null) {
			this.iterationAccumulators = IterationAccumulatorBroker.instance().get(brokerKey());
		}
		return this.iterationAccumulators;
	}

	protected void verifyEndOfSuperstepState() throws IOException {
		// sanity check that there is at least one iterative input reader
		if (this.iterativeInputs.length == 0 && this.iterativeBroadcastInputs.length == 0) {
			throw new IllegalStateException("Error: Iterative task without a single iterative input.");
		}

		for (int inputNum : this.iterativeInputs) {
			MutableReader<?> reader = this.inputReaders[inputNum];

			if (!reader.isFinished()) {
				if (reader.hasReachedEndOfSuperstep()) {
					reader.startNextSuperstep();
				}
				else {
					// need to read and drop all non-consumed data until we reach the end-of-superstep
					@SuppressWarnings("unchecked")
					MutableObjectIterator<Object> inIter = (MutableObjectIterator<Object>) this.inputIterators[inputNum];
					Object o = this.inputSerializers[inputNum].getSerializer().createInstance();
					while ((o = inIter.next(o)) != null);
					
					if (!reader.isFinished()) {
						// also reset the end-of-superstep state
						reader.startNextSuperstep();
					}
				}
			}
		}
		
		for (int inputNum : this.iterativeBroadcastInputs) {
			MutableReader<?> reader = this.broadcastInputReaders[inputNum];

			if (!reader.isFinished()) {
				
//				// HACK! TODO
//				if(!reader.hasReachedEndOfSuperstep() && config.getIterationRetry() > 0) {
////					MutableReader<IOReadableWritable> reader2 = (MutableReader<IOReadableWritable>) reader;
////					IOReadableWritable target = null;
////					try {
////						while(reader2.next(target)) {}
////					} catch (InterruptedException e) {
////						// TODO Auto-generated catch block
////						e.printStackTrace();
////					}
//					reader.clearBuffers();
//				}
				
				// sanity check that the BC input is at the end of the superstep
				if (!reader.hasReachedEndOfSuperstep() && config.getIterationRetry() == 0) {
					throw new IllegalStateException("An iterative broadcast input has not been fully consumed.");
				}
				
				reader.startNextSuperstep();
			}
		}
	}

	@Override
	public boolean terminationRequested() {
		return this.terminationRequested;
	}

	@Override
	public void requestTermination() {
		this.terminationRequested = true;
	}

	@Override
	public void cancel() throws Exception {
		requestTermination();
		super.cancel();
	}

	// -----------------------------------------------------------------------------------------------------------------
	// Iteration State Update Handling
	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Creates a new {@link WorksetUpdateOutputCollector}.
	 * <p>
	 * This collector is used by {@link IterationIntermediatePactTask} or {@link IterationTailPactTask} to update the
	 * workset.
	 * <p>
	 * If a non-null delegate is given, the new {@link Collector} will write to the solution set and also call
	 * collect(T) of the delegate.
	 *
	 * @param delegate null -OR- the delegate on which to call collect() by the newly created collector
	 * @return a new {@link WorksetUpdateOutputCollector}
	 */
	protected Collector<OT> createWorksetUpdateOutputCollector(Collector<OT> delegate) {
		DataOutputView outputView = worksetBackChannel.getWriteEnd();
		TypeSerializer<OT> serializer = getOutputSerializer();
		return new WorksetUpdateOutputCollector<OT>(outputView, serializer, delegate);
	}

	protected Collector<OT> createWorksetUpdateOutputCollector() {
		return createWorksetUpdateOutputCollector(null);
	}

	/**
	 * Creates a new solution set update output collector.
	 * <p>
	 * This collector is used by {@link IterationIntermediatePactTask} or {@link IterationTailPactTask} to update the
	 * solution set of workset iterations. Depending on the task configuration, either a fast (non-probing)
	 * {@link org.apache.flink.runtime.iterative.io.SolutionSetFastUpdateOutputCollector} or normal (re-probing)
	 * {@link SolutionSetUpdateOutputCollector} is created.
	 * <p>
	 * If a non-null delegate is given, the new {@link Collector} will write back to the solution set and also call
	 * collect(T) of the delegate.
	 *
	 * @param delegate null -OR- a delegate collector to be called by the newly created collector
	 * @return a new {@link org.apache.flink.runtime.iterative.io.SolutionSetFastUpdateOutputCollector} or
	 * {@link SolutionSetUpdateOutputCollector}
	 */
	protected Collector<OT> createSolutionSetUpdateOutputCollector(Collector<OT> delegate) {
		Broker<Object> solutionSetBroker = SolutionSetBroker.instance();
		
		Object ss = solutionSetBroker.get(brokerKey());
		if (ss instanceof CompactingHashTable) {
			@SuppressWarnings("unchecked")
			CompactingHashTable<OT> solutionSet = (CompactingHashTable<OT>) ss;
			TypeSerializer<OT> serializer = getOutputSerializer();
			return new SolutionSetUpdateOutputCollector<OT>(solutionSet, serializer, delegate);
		}
		else if (ss instanceof JoinHashMap) {
			@SuppressWarnings("unchecked")
			JoinHashMap<OT> map = (JoinHashMap<OT>) ss;
			return new SolutionSetObjectsUpdateOutputCollector<OT>(map, delegate);
		} else {
			throw new RuntimeException("Unrecognized solution set handle: " + ss);
		}
	}

	/**
	 * @return output serializer of this task
	 */
	protected TypeSerializer<OT> getOutputSerializer() {
		TypeSerializerFactory<OT> serializerFactory;

		if ((serializerFactory = getLastTasksConfig().getOutputSerializer(getUserCodeClassLoader())) ==
				null) {
			throw new RuntimeException("Missing output serializer for workset update.");
		}

		return serializerFactory.getSerializer();
	}
	
	protected void infuseRecoveryRecords() throws ClassNotFoundException, IOException {
		
		if(this.config.getRefinedRecoveryStart() <= currentIteration() &&
				this.config.getRefinedRecoveryEnd() >= currentIteration()) {
		
			TypeInformation<OT> ti = (TypeInformation<OT>) this.config.getOutputType(1, getUserCodeClassLoader());
			if(this.config.getInfusedOutputPath().length > 0) {
				
				String[] infusePaths = this.config.getInfusedOutputPath();
				
				for(String infusePath : infusePaths) {
					if(infusePath == "") {
						continue;
					}
					
					CsvInputFormat<OT>  csvinfusing = new CsvInputFormat<OT>(new Path(
							infusePath.replaceAll("%ITERATION%", ""+this.currentIteration())), ti);
					
					System.out.println("infused "+infusePath.replaceAll("%ITERATION%", ""+this.currentIteration()));
					
					for(FileInputSplit fis : csvinfusing.createInputSplits(1)) {
						csvinfusing.open(fis);
						OT record = ti.createSerializer(getExecutionConfig()).createInstance();
						while(!csvinfusing.reachedEnd()) {
							if((record = csvinfusing.nextRecord(record)) != null) {
								//System.out.println("infused "+record);
								this.output.collect(record);
							}
						}
					}
				}
			}
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	private class IterativeRuntimeUdfContext extends DistributedRuntimeUDFContext implements IterationRuntimeContext {

		public IterativeRuntimeUdfContext(String name, int numParallelSubtasks, int subtaskIndex, ClassLoader userCodeClassLoader, ExecutionConfig executionConfig) {
			super(name, numParallelSubtasks, subtaskIndex, userCodeClassLoader, executionConfig);
		}

		@Override
		public int getSuperstepNumber() {
			return AbstractIterativePactTask.this.superstepNum;
		}

		public <V, A extends Serializable> void addIterationAccumulator(String name, Accumulator <V, A>  accumulator) {
			getIterationAccumulators().addAccumulator(name, accumulator);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
			return (Accumulator<V, A>) getIterationAccumulators().getAccumulator(name);
		}

		@SuppressWarnings("unchecked")
		public <T extends Accumulator<?, ? extends Serializable>> T getPreviousIterationAccumulator(String name) {
			return (T) getIterationAccumulators().getPreviousGlobalAccumulator(name);
		}
	}

}
