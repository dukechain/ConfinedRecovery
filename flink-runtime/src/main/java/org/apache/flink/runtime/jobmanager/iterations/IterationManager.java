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

package org.apache.flink.runtime.jobmanager.iterations;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.ConvergenceCriterion;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorEvent;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.iterative.task.AbstractIterativePactTask;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.accumulators.AccumulatorManager;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.operators.DataSourceTask;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.RecoveryUtil;

import akka.actor.ActorRef;

import com.google.common.base.Preconditions;

/**
* Manages the supersteps of one iteration. The JobManager is holding one IterationManager for every iteration that is 
* currently running.
*
*/
public class IterationManager {

	private static final Log log = LogFactory.getLog(IterationManager.class);
	
	private JobID jobId;
	
	private int iterationId;
	
	int numberOfEventsUntilEndOfSuperstep;
	
	int numberOfTails;
	
	int parallelism;
	
	int maxNumberOfIterations;
	
	int currentIteration = 1; // count starts at 1, not 0
	
	private int workerDoneEventCounter = 0;
	
	private ConvergenceCriterion<Object> convergenceCriterion;
	
	private String convergenceAccumulatorName;
	
	private boolean endOfSuperstep = false;
	
	private AccumulatorManager accumulatorManager;
	
	private Set<String> iterationAccumulators;
	
	private final ActorRef jobManagerRef;
	
	private final JobManager jobManager;
	 
	private CopyOnWriteArrayList<ActorRef> workers = new CopyOnWriteArrayList<ActorRef>();
	
	private JobGraph jobGraph;
	
	private LibraryCacheManager libraryCacheManager;
	
	public AtomicBoolean running = new AtomicBoolean(false);
	
	private AtomicBoolean initRerun = new AtomicBoolean(false);
	
	private Integer lastCheckpoint = 0;
	
	private Integer nextCheckpoint = 0;
	
	private int retries = 0;
	
	public IterationManager(JobID jobId, int iterationId, int parallelism, int maxNumberOfIterations, 
			AccumulatorManager accumulatorManager, ActorRef jobManagerRef, JobManager jobManager,
			JobGraph jobGraph, LibraryCacheManager libraryCacheManager) throws IOException {
		Preconditions.checkArgument(parallelism > 0);
		this.jobId = jobId;
		this.iterationId = iterationId;
		this.numberOfEventsUntilEndOfSuperstep = parallelism;
		this.parallelism = parallelism;
		this.numberOfTails = 1;
		this.maxNumberOfIterations = maxNumberOfIterations;
		this.accumulatorManager = accumulatorManager;
		this.iterationAccumulators = new HashSet<String>();
		this.jobManagerRef =  jobManagerRef;
		this.jobManager = jobManager;
		this.jobGraph = jobGraph;
		this.libraryCacheManager = libraryCacheManager;
	}
	
	/**
	 * Is called once the JobManager receives a WorkerDoneEvent by RPC call from one node
	 */
	public synchronized void receiveWorkerDoneEvent(Map<String, Accumulator<?, ?>> accumulators, ActorRef sender, boolean checkpoint) {
		
		// sanity check
		if (this.endOfSuperstep) {
			throw new RuntimeException("Encountered WorderDoneEvent when still in End-of-Superstep status.");
		}
		
		workerDoneEventCounter++;
		
		this.running.set(true);
		
		// process accumulators
		this.accumulatorManager.processIncomingAccumulators(jobId, accumulators);
		
		// add accumulators to managed set to have a notion which accumulators belong to this iteration
		for(String accumulatorName : accumulators.keySet()) {
			this.iterationAccumulators.add(accumulatorName);
		}
		
		// add all senders
		if(this.workers.size() < numberOfEventsUntilEndOfSuperstep) {
			this.workers.add(sender);
		}
		
		// set checkpoints
		if(checkpoint) {
			synchronized (nextCheckpoint) {
				if(nextCheckpoint != currentIteration) {
					lastCheckpoint = nextCheckpoint;
					nextCheckpoint = currentIteration;
				}
			}
		}
		
		// if all workers have sent their WorkerDoneEvent -> end of superstep
		if (workerDoneEventCounter % numberOfEventsUntilEndOfSuperstep == 0) {
			endOfSuperstep = true;
			handleEndOfSuperstep();
		}
	}
	
	/**
	 * Handles the end of one superstep. If convergence is reached it sends a termination request to all connected workers.
	 * If not it initializes the next superstep by sending an AllWorkersDoneEvent (with aggregators) to all workers.
	 */
	private void handleEndOfSuperstep() {
		if (log.isInfoEnabled()) {
			log.info("finishing iteration [" + currentIteration + "]");
		}

		if (checkForConvergence()) {

			if (log.isInfoEnabled()) {
				log.info("signaling that all workers are to terminate in iteration ["+ currentIteration + "]");
			}
			
			// Send termination to all workers
			for(ActorRef worker : this.workers) {
				worker.tell(new JobManagerMessages.InitIterationTermination(), this.jobManagerRef);
			}
			
			//this.timeout = null;
			this.running.set(false);

		} else {

			System.out.println("SIGNAL NEXT");
			
			if (log.isInfoEnabled()) {
				log.info("signaling that all workers are done in iteration [" + currentIteration+ "]");
			}

			// important for sanity checking
			resetEndOfSuperstep();
			
			// copy Accumulators for sending to reset locals correctly
			AccumulatorEvent accEvent = new AccumulatorEvent(this.jobId, getManagedAccumulators());
			final AccumulatorEvent copiedEvent;
			
			try {
				copiedEvent = (AccumulatorEvent) InstantiationUtil.createCopy(accEvent);
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Could not create copy of AccumulatorEvent for next superstep");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Could not create copy of AccumulatorEvent for next superstep");
			}
			
			
			// initiate next iteration for all workers
			for(ActorRef worker : this.workers) {
				worker.tell(new JobManagerMessages.InitNextIteration(copiedEvent), this.jobManagerRef);
			}
			
			currentIteration++;

			resetManagedAccumulators();
		}
		
		workers.clear();
	}
	
	public boolean isEndOfSuperstep() {
		return this.endOfSuperstep;
	}
	
	public void resetEndOfSuperstep() {
		this.endOfSuperstep = false;
	}
	
	public void setConvergenceCriterion(String convergenceAccumulatorName, ConvergenceCriterion<Object> convergenceCriterion) {
		this.convergenceAccumulatorName = convergenceAccumulatorName;
		this.convergenceCriterion = convergenceCriterion;
	}
	
	/**
	 * Checks if either we have reached maxNumberOfIterations or if a associated ConvergenceCriterion is converged
	 */
	private boolean checkForConvergence() {
		
		System.out.println(currentIteration+ " / "+maxNumberOfIterations);
		
		if (maxNumberOfIterations == currentIteration) {
			if (log.isInfoEnabled()) {
				log.info("maximum number of iterations [" + currentIteration+ "] reached, terminating...");
			}
			return true;
		}

		if (convergenceAccumulatorName != null) {

			Accumulator<?, ? extends Object> acc = this.accumulatorManager.getJobAccumulators(jobId).get(convergenceAccumulatorName);

			if (acc == null) {
				throw new RuntimeException("Error: Accumulator for convergence criterion was null.");
			}
			
			Object aggregate = acc.getLocalValue();

			if (convergenceCriterion.isConverged(currentIteration, aggregate)) {
				if (log.isInfoEnabled()) {
					log.info("convergence reached after [" + currentIteration + "] iterations, terminating...");
				}
				return true;
			}
		}
		
		return false;
	}
	
	public JobID getJobId() {
		return jobId;
	}

	public int getIterationId() {
		return iterationId;
	}
	
	public void setNumberOfTails(int number) {
		this.numberOfTails = number;
		this.numberOfEventsUntilEndOfSuperstep = this.numberOfTails * this.parallelism;
	}
	
	/**
	 * Utility method used to reset the managed accumulators of this iteration after a superstep
	 */
	private void resetManagedAccumulators() {
		
		Iterator<Map.Entry<String, Accumulator<?, ?>>> it = this.accumulatorManager.getJobAccumulators(jobId).entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Accumulator<?, ?>> pair = (Map.Entry<String, Accumulator<?, ?>>) it.next();
			
			// is this accumulator managed by this iteration?
			if(this.iterationAccumulators.contains(pair.getKey())) {
				pair.getValue().resetLocal();
			}
		}
	}
	
	/**
	 * Utility method to retrieve a Map of all managed accumulators of this iteration.
	 * Used for the report back to the task managers
	 */
	private Map<String, Accumulator<?, ?>> getManagedAccumulators() {
		
		Map<String, Accumulator<?, ?>> managedAccumulators = new HashMap<String, Accumulator<?, ?>>();
		
		Iterator<Map.Entry<String, Accumulator<?, ?>>> it = this.accumulatorManager.getJobAccumulators(jobId).entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Accumulator<?, ?>> pair = (Map.Entry<String, Accumulator<?, ?>>) it.next();
			
			// is this accumulator managed by this iteration?
			if(this.iterationAccumulators.contains(pair.getKey())) {
				managedAccumulators.put(pair.getKey(), pair.getValue());
			}
		}
		
		return managedAccumulators;
	}
	
	/**
	 * This method initiates the recovery process of the iteration
	 * It cancels all remaining tasks in the plan and starts a new execution
	 * from the last checkpoint
	 */
	public void initRecovery() {
		
		if(initRerun.compareAndSet(false, true)) {

			// find iteration vertex
			AbstractJobVertex iterationVertex = null;
			for(AbstractJobVertex vertex : jobGraph.getVertices()) {
				if(vertex.getInvokableClassName()
						.equalsIgnoreCase("org.apache.flink.runtime.iterative.task.IterationHeadPactTask")) {
					
					TaskConfig taskConfig = new TaskConfig(vertex.getConfiguration());
					// found right iteration head
					if(taskConfig.getIterationId() == this.iterationId) {
						iterationVertex = vertex;
					}
				}
			}
			
			// here to avoid concurrent modification exception
			TaskConfig iterationTaskConfig = new TaskConfig(iterationVertex.getConfiguration());
			
			String checkpointPath = RecoveryUtil.getCheckpointPath()+"checkpoint"; //+iterationVertex.getName().trim();
			
			if(retries > 0) {
				checkpointPath += retries;
			}
			
			// save distribution pattern
			DistributionPattern dp = iterationVertex.getInputs().get(0).getDistributionPattern();
			ResultPartitionType rpt = iterationVertex.getInputs().get(0).getSource().getResultType();
			
			TaskConfig sourceConfig = new TaskConfig(iterationVertex.getInputs().get(0).getSource().getProducer().getConfiguration());
			if(sourceConfig.getNumberOfChainedStubs() > 0) {
				sourceConfig = sourceConfig.getChainedStubConfig(0);
			}
			
			ClassLoader cl = this.libraryCacheManager.getClassLoader(jobId);

			InputFormatVertex checkpoint;
			InputFormatVertex checkpointSolutionSet = null;
			try {
				// create new checkpoint source to attach in front of the iteration
				checkpoint = createCheckpointInput(jobGraph, checkpointPath+"_"+lastCheckpoint+"/", this.parallelism, 
						iterationTaskConfig.getOutputSerializer(cl), iterationTaskConfig.getOutputType(1, cl), 
						sourceConfig.getOutputShipStrategy(0), sourceConfig.getOutputComparator(0, cl),
						sourceConfig.getOutputDataDistribution(0, cl), sourceConfig.getOutputPartitioner(0, cl));
				
				jobGraph.addVertex(checkpoint);
				
				// create second input for solution set
				if(iterationTaskConfig.getIsWorksetIteration()) {
					
					sourceConfig = new TaskConfig(iterationVertex.getInputs().get(1).getSource().getProducer().getConfiguration());
					if(sourceConfig.getNumberOfChainedStubs() > 0) {
						sourceConfig = sourceConfig.getChainedStubConfig(0);
					}
					
					String checkpointSolutionSetPath = RecoveryUtil.getCheckpointPath()+"deltacheckpoint_"+lastCheckpoint+"/";
					checkpointSolutionSet = createCheckpointInput(jobGraph, checkpointSolutionSetPath, this.parallelism, 
							iterationTaskConfig.getOutputSerializer(cl), iterationTaskConfig.getOutputType(1, cl), 
							sourceConfig.getOutputShipStrategy(0), sourceConfig.getOutputComparator(0, cl),
							sourceConfig.getOutputDataDistribution(0, cl), sourceConfig.getOutputPartitioner(0, cl));
					
					jobGraph.addVertex(checkpointSolutionSet);
				}
				
				// remove everything before the iteration
				for(JobEdge edge : iterationVertex.getInputs()) {
					removePredecessors(edge.getSource().getProducer());
				}
				
				// clear iteration inputs
				iterationVertex.getInputs().clear();
				
				// connect checkpoint as new input
				iterationVertex.connectNewDataSetAsInput(checkpoint, dp, rpt);
				
				// connect second input for solution set
				if(iterationTaskConfig.getIsWorksetIteration()) {
					iterationVertex.connectNewDataSetAsInput(checkpointSolutionSet, dp, rpt);
				}
				
				iterationTaskConfig.setNumberOfIterations(maxNumberOfIterations - currentIteration + 1);		
				
				// generate new job id
				jobGraph.setJobID(JobID.generate());
				
				// adjust parallelism
				for(AbstractJobVertex v : jobGraph.getVertices()) {
					
					// dont change all reduces
					if(v.getParallelism() == this.parallelism && this.parallelism > 1) {
						v.setParallelism(v.getParallelism() - 1);
					}
					
					// re initialize
					try {
						v.initializeOnMaster(libraryCacheManager.getClassLoader(jobId));
						}
					catch(Throwable t) {
						throw new RuntimeException(
								"Cannot initialize checkpoint task : " + t.getMessage(), t);
						}
					
					// continue superstep where stopped
					if(v.getClass().isAssignableFrom(AbstractIterativePactTask.class)) {
						iterationTaskConfig.setStartIteration(lastCheckpoint);
					}
				}
				
				// adjust state of this iteration manager
				this.parallelism--;
				this.numberOfEventsUntilEndOfSuperstep = this.numberOfTails * this.parallelism;
				this.workers.clear();
				this.retries++;
				this.currentIteration--;
				this.workerDoneEventCounter = 0;
				
				// adjust checkpoint vertices
				for(IntermediateDataSet ds : iterationVertex.getProducedDataSets()) {
					for(JobEdge e : ds.getConsumers()) {
						if(e.getTarget().getName().toLowerCase().contains("checkpoint")) {
							TaskConfig taskConfig = new TaskConfig(e.getTarget().getConfiguration());
							taskConfig.setIterationRetry(retries);
						}
					}
				}
				
				System.out.println("submit");
				
				try {
					jobManager.currentJobs().get(jobId).get()._1.restartHard(jobGraph.getVerticesSortedTopologicallyFromSources());
				} catch (InvalidProgramException e) {                                                                                                                                                                                                                                                   
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JobException e) {
					e.printStackTrace();
					throw new RuntimeException("Something went wrong during the restart of the recovery");
				}
				
				//jobManager.submitJob(jobGraph, false);
				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Something went wrong during recovery");
			}
		}
	}
	
	private static <X> InputFormatVertex createCheckpointInput(JobGraph jobGraph, String checkpointPath, 
			int numSubTasks, TypeSerializerFactory<?> serializer, TypeInformation<X> typeInfo, ShipStrategyType shipStrategy,
			TypeComparatorFactory<?> comparator, DataDistribution dataDistribution, Partitioner<?> partitioner) {
		
		CsvInputFormat<X> pointsInFormat = new CsvInputFormat<X>(new Path(checkpointPath), typeInfo);
		InputFormatVertex pointsInput = createInput(pointsInFormat, checkpointPath, "[CHECKPOINT] path: "+checkpointPath, jobGraph, numSubTasks);
		{
			TaskConfig taskConfig = new TaskConfig(pointsInput.getConfiguration());
			taskConfig.addOutputShipStrategy(shipStrategy);
			taskConfig.setOutputSerializer(serializer);
			System.out.println(shipStrategy);
			if(comparator != null) {
				System.out.println(comparator);
				taskConfig.setOutputComparator(comparator, 0);
			}
			if(dataDistribution != null) {
				System.out.println(dataDistribution);
				taskConfig.setOutputDataDistribution(dataDistribution, 0);
			}
			if(partitioner != null) {
				System.out.println(partitioner);
				taskConfig.setOutputPartitioner(partitioner, 0);
			}
		}
		return pointsInput;
	}
	
	// from jobgraphutil
	public static <T extends FileInputFormat<?>> InputFormatVertex createInput(T stub, String path, String name, JobGraph graph,
			int parallelism)
	{
		stub.setFilePath(path);
		return createInput(new UserCodeObjectWrapper<T>(stub), name, graph, parallelism);
	}

	private static <T extends InputFormat<?,?>> InputFormatVertex createInput(UserCodeWrapper<T> stub, String name, JobGraph graph,
			int parallelism)
	{
		InputFormatVertex inputVertex = new InputFormatVertex(name);
		
		inputVertex.setInvokableClass(DataSourceTask.class);
		inputVertex.setParallelism(parallelism);

		TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());
		inputConfig.setStubWrapper(stub);
		
		return inputVertex;
	}
	
	private void removePredecessors(AbstractJobVertex vertex) {
		if(vertex.getInputs() != null && vertex.getInputs().size() > 0) {
			for(JobEdge edge : vertex.getInputs()) {
				removePredecessors(edge.getSource().getProducer());
			}
		}
		jobGraph.removeVertex(vertex);
	}
}