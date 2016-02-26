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

import static akka.dispatch.Futures.future;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorEvent;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.Instance;
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
	
	int parallelismAtStart;
	
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
	
	private Map<Integer, Instance> deadInstances = new HashMap<Integer, Instance>();
	
	public IterationManager(JobID jobId, int iterationId, int parallelism, int maxNumberOfIterations, 
			AccumulatorManager accumulatorManager, ActorRef jobManagerRef, JobManager jobManager,
			JobGraph jobGraph, LibraryCacheManager libraryCacheManager) throws IOException {
		Preconditions.checkArgument(parallelism > 0);
		this.jobId = jobId;
		this.iterationId = iterationId;
		this.numberOfEventsUntilEndOfSuperstep = parallelism;
		this.parallelism = parallelism;
		this.parallelismAtStart = parallelism;
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
		
		synchronized (nextCheckpoint) {
				lastCheckpoint = nextCheckpoint;
		}
		
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

			System.out.println("SIGNAL NEXT "+currentIteration);
			
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
			
			System.out.println("IS CONVERGED maxNumberOfIterations "+currentIteration);
			
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
				
				System.out.println("IS CONVERGED convergenceCriterion "+currentIteration);
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
		
		System.out.println("Init Recovery");
		
		if(initRerun.compareAndSet(false, true)) {
			
			ExecutionGraph eg = jobManager.currentJobs().get(jobId).get()._1;

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
			if(iterationTaskConfig.getNumberOfChainedStubs() > 0) {
				iterationTaskConfig = iterationTaskConfig.getChainedStubConfig(iterationTaskConfig.getNumberOfChainedStubs()-1);
			}
			
			
			// Construct checkpointPath
			String checkpointPath = RecoveryUtil.getCheckpointPath()+"checkpoint"; //+iterationVertex.getName().trim();
			// TODO handle this nice
//			if(retries > 0) {
//				checkpointPath += retries;
//			}
			checkpointPath += "_"+lastCheckpoint+"/";
			
			// save distribution pattern
			DistributionPattern dp = iterationVertex.getInputs().get(0).getDistributionPattern();
			ResultPartitionType rpt = iterationVertex.getInputs().get(0).getSource().getResultType();
			
			// this is used to find out if we iterate over bc variables
			// TODO Check if this really works in all cases
			boolean bcIteration = false;
			
			boolean replication_recovery_enable = GlobalConfiguration.getBoolean(
					ConfigConstants.Replication_RECOVERY, ConfigConstants.Replication_RECOVERY_DEFAULT);
			
			if(iterationTaskConfig.getOutputShipStrategy(0).equals(ShipStrategyType.BROADCAST)
					&& replication_recovery_enable) {
				bcIteration = true;
			}
			
//			if(iterationVertex.getProducedDataSets().get(0).getConsumers()
//					.get(0).getDistributionPattern().equals(DistributionPattern.POINTWISE)) {
//				anyRegularOutput = true;
//			}
			
//			for(IntermediateDataSet e : iterationVertex.getProducedDataSets()) {
//				if(e.getConsumers().get(0).getDistributionPattern().equals(DistributionPattern.POINTWISE)) {
//					anyRegularOutput = true;
//				}
//			}
//			
//			anyRegularOutput = false;
//			
			TaskConfig sourceConfig = new TaskConfig(iterationVertex.getInputs().get(0).getSource().getProducer().getConfiguration());
			if(sourceConfig.getNumberOfChainedStubs() > 0) {
				sourceConfig = sourceConfig.getChainedStubConfig(sourceConfig.getNumberOfChainedStubs()-1);
			}
			
			ClassLoader cl = this.libraryCacheManager.getClassLoader(jobId);

			InputFormatVertex checkpoint;
			InputFormatVertex checkpointSolutionSet = null;
			boolean refinedRecovery = 
					GlobalConfiguration.getBoolean(ConfigConstants.CONFINED_RECOVERY, ConfigConstants.CONFINED_RECOVERY_DEFAULT);
			
			System.out.println("refined? "+refinedRecovery);
			
			//Map<Integer, Instance> deadInstances = new HashMap<Integer, Instance>();
			
			try {
				
				// detect dead instances
				ExecutionJobVertex iterationEjv = eg.getJobVertex(iterationVertex.getID());
				for(ExecutionVertex ev :  iterationEjv.getTaskVertices())  {
					if(ev.getCurrentExecutionAttempt().getAssignedResource() != null &&
							!ev.getCurrentExecutionAttempt().getAssignedResource().getInstance().isAlive()) {
						
						if(!deadInstances.containsKey(ev.getParallelSubtaskIndex() + 1)) {
							deadInstances.put(ev.getParallelSubtaskIndex() + 1, 
									ev.getCurrentExecutionAttempt().getAssignedResource().getInstance());
						}
						
					}
				}
				
				System.out.println("dead Instances? "+deadInstances.size());
				
				// have to create new checkpoint source?
				if(!bcIteration) {
					
					
					// remove everything before the iteration
					for(JobEdge edge : iterationVertex.getInputs()) {
						removePredecessors(edge.getSource().getProducer(), edge.getSource());
					}
					
					// clear iteration inputs
					iterationVertex.getInputs().clear();
					
					// in refined recovery move relevant splits to subfolder
					if(refinedRecovery) {
						System.out.println("moving files for refined recovery");
						FileSystem fs = FileSystem.get((new Path(checkpointPath)).toUri());
						String dirPath = new URI(checkpointPath+"load").getPath();
						fs.mkdirs(new Path(dirPath));
						System.out.println("mkdirs "+dirPath);
						
						for(Integer queueToRequest: deadInstances.keySet()) {
							String srcPath = new URI(checkpointPath+queueToRequest).getPath();
							String destPath = new URI(checkpointPath+"load/"+queueToRequest).getPath();
							System.out.println(srcPath+" to "+destPath);
							fs.rename(new Path(srcPath), new Path(destPath));
						}
						checkpointPath += "load/";
					}
					
					// create one input
					
					System.out.println("Checkpoint "+checkpointPath);
					System.out.println("iterationTaskConfig "+iterationTaskConfig.getConfiguration());
					System.out.println("sourceConfig "+sourceConfig.getConfiguration());
					System.out.println("iterationTaskConfig.getOutputSerializer(cl) "+iterationTaskConfig.getOutputSerializer(cl));
					System.out.println("iterationTaskConfig.getOutputType(1, cl) "+iterationTaskConfig.getOutputType(1, cl));
					System.out.println("sourceConfig.getOutputShipStrategy(0) "+sourceConfig.getOutputShipStrategy(0));
					System.out.println("ourceConfig.getOutputComparator(0, cl) "+sourceConfig.getOutputComparator(0, cl));
					System.out.println("ourceConfig.getOutputDataDistribution(0, cl) "+sourceConfig.getOutputDataDistribution(0, cl));
					System.out.println("sourceConfig.getOutputPartitioner(0, cl) "+sourceConfig.getOutputPartitioner(0, cl));
			
					// create new checkpoint source to attach in front of the iteration
					checkpoint = createCheckpointInput(jobGraph, checkpointPath, this.parallelism, 
							iterationTaskConfig.getOutputSerializer(cl), iterationTaskConfig.getOutputType(1, cl), 
							sourceConfig.getOutputShipStrategy(0), sourceConfig.getOutputComparator(0, cl),
							sourceConfig.getOutputDataDistribution(0, cl), sourceConfig.getOutputPartitioner(0, cl));
					
					System.out.println("createdCheckpointInput");
					
					jobGraph.addVertex(checkpoint);
					
					System.out.println("addedVertex");
					
					
					// Hack to reestablish hash partitioning when required
					System.out.println("Hack to reestablish hash partitioning when required");
					
					// dont change output strategy if it is already shuffling
					if(iterationTaskConfig.getOutputShipStrategy(0).equals(ShipStrategyType.FORWARD)) {
						iterationTaskConfig.setOutputShipStrategy(sourceConfig.getOutputShipStrategy(0), 0);
						// if source has a comparator we probably need it to reestablish the outgoing partitioning
						if(sourceConfig.getOutputComparator(0, cl) != null) {
							iterationTaskConfig.setOutputComparator(sourceConfig.getOutputComparator(0, cl), 0);
						}
						
						System.out.println("setOutputShipStrategy "+sourceConfig.getOutputShipStrategy(0));
						System.out.println("set output comparator "+sourceConfig.getOutputComparator(0, cl));
						
						for(JobEdge jee : iterationVertex.getProducedDataSets().get(0).getConsumers()) {
							System.out.println(jee);
							if(sourceConfig.getOutputShipStrategy(0).equals(ShipStrategyType.FORWARD)) {
								jee.setDistributionPattern(DistributionPattern.POINTWISE);
							}
							else {
								jee.setDistributionPattern(DistributionPattern.ALL_TO_ALL);
							}
						}
					}

//						for(IntermediateDataSet idds : iterationVertex.getProducedDataSets()) {
//							System.out.println(idds);
//							for(JobEdge jee : idds.getConsumers()) {
//								System.out.println(jee);
//								if(sourceConfig.getOutputShipStrategy(0).equals(ShipStrategyType.FORWARD)) {
//									jee.setDistributionPattern(DistributionPattern.POINTWISE);
//								}
//								else {
//									jee.setDistributionPattern(DistributionPattern.ALL_TO_ALL);
//								}
//							}
//						}
					
					// create second input for solution set
					if(iterationTaskConfig.getIsWorksetIteration()) {
						
						sourceConfig = new TaskConfig(iterationVertex.getInputs().get(1).getSource().getProducer().getConfiguration());
						if(sourceConfig.getNumberOfChainedStubs() > 0) {
							sourceConfig = sourceConfig.getChainedStubConfig(0);
						}
						
						String checkpointSolutionSetPath = RecoveryUtil.getCheckpointPath()+"deltacheckpoint_"+lastCheckpoint+"/";
						
						// in refined recovery move relevant splits to subfolder
						if(refinedRecovery) {
							System.out.println("moving delta files for refined recovery");
							FileSystem fs = FileSystem.get((new Path(checkpointPath)).toUri());
							String dirPath = new URI(checkpointSolutionSetPath+"load").getPath();
							fs.mkdirs(new Path(dirPath));
							System.out.println("mkdirs "+dirPath);
							
							for(Integer queueToRequest: deadInstances.keySet()) {
								String srcPath = new URI(checkpointSolutionSetPath+queueToRequest).getPath();
								String destPath = new URI(checkpointSolutionSetPath+"load/").getPath();
								System.out.println(srcPath+" to "+destPath);
								fs.rename(new Path(srcPath), new Path(destPath));
							}
							checkpointSolutionSetPath += "load/";
						}
						
						System.out.println("checkpointSolutionSetPath "+checkpointSolutionSetPath);
						
						checkpointSolutionSet = createCheckpointInput(jobGraph, checkpointSolutionSetPath, this.parallelism, 
								iterationTaskConfig.getOutputSerializer(cl), iterationTaskConfig.getOutputType(1, cl), 
								sourceConfig.getOutputShipStrategy(0), sourceConfig.getOutputComparator(0, cl),
								sourceConfig.getOutputDataDistribution(0, cl), sourceConfig.getOutputPartitioner(0, cl));
						
						jobGraph.addVertex(checkpointSolutionSet);
					}
					
					System.out.println("connectNewDataSetAsInput");
					// connect checkpoint as new input
					iterationVertex.connectNewDataSetAsInput(checkpoint, dp, rpt);
					
					// connect second input for solution set
					if(iterationTaskConfig.getIsWorksetIteration()) {
						iterationVertex.connectNewDataSetAsInput(checkpointSolutionSet, dp, rpt);
					}
						
					System.out.println("Checkpoints created and connected");
				
					//iterationTaskConfig.setNum(iterationTaskConfig.getNumInputs() + deadInstances.size() - 1);
					//iterationTaskConfig.setNumberOfIterations(maxNumberOfIterations - currentIteration + 1);
					
					
					if(refinedRecovery) {
						System.out.println("Looking for logs to infuse");
						for(ExecutionJobVertex ejv : eg.getAllVertices().values()) {
							
							for(ExecutionVertex ev : ejv.getTaskVertices())  {
								// to check: instance dead, inside iteration, dynamic path, all to all output distribution
								
								// instance dead?
								if(ev.getCurrentExecutionAttempt().getAssignedResource() != null &&
										!ev.getCurrentExecutionAttempt().getAssignedResource().getInstance().isAlive()) {
									
									System.out.println("dead "+ev);
									
									// output dynamic (on dynamic path)?
									if(ejv.getJobVertex().insideIteration()) {
										
										System.out.println("inside iteration "+ev);
										
										//int num = 0;
										for(IntermediateDataSet ids : ejv.getJobVertex().getProducedDataSets()) {
											
											for(JobEdge e : ids.getConsumers()) {
												
												System.out.println(ejv+" "+e.getDistributionPattern());
												
												// ALL_TO_ALL distribution?
												if(e.getDistributionPattern().equals(DistributionPattern.ALL_TO_ALL)) {
													
													System.out.println("ALL_TO_ALL "+e);
													
													//int outputSize = ev.getOutputSize();
													// currently it is assumed that there dop = 1 * nodes
													int queueToRequest = ev.getParallelSubtaskIndex();// % outputSize;
	
													String path = RecoveryUtil.getLoggingPath()+"/flinklog_"+ids.getId()+"_"+queueToRequest+"_%ITERATION%";
													
													System.out.println("infuse "+path+" at "+e.getSource().getProducer());
													
													// set infusing path
													TaskConfig tc = new TaskConfig(e.getSource().getProducer().getConfiguration());
													tc.setInfusedOutputPath(path);
													tc.setRefinedRecoveryLostNode(queueToRequest);
												}
											}
											//num++;
										}
									}
								}
							}
						}
					}
					
					// generate new job id
					jobGraph.setJobID(JobID.generate());
					
					this.retries++;
					
					System.out.println("adjusting parallelism");
					
					// adjust parallelism
					for(AbstractJobVertex v : jobGraph.getVertices()) {
						
						// dont change all reduces
						if(v.getParallelism() == this.parallelism && this.parallelism > 1) {
							//v.setParallelism(v.getParallelism() - deadInstances.size());
							v.setParallelism(this.parallelismAtStart - deadInstances.size());
							
						}
						
						System.out.println("re init "+v);
						// re initialize
						try {
							v.initializeOnMaster(libraryCacheManager.getClassLoader(jobId));
							}
						catch(Throwable t) {
							throw new RuntimeException(
									"Cannot initialize checkpoint task : " + t.getMessage(), t);
							}
						
						TaskConfig vConfig = new TaskConfig(v.getConfiguration());
						
						// continue superstep where stopped
						if(v.getInvokableClassName().toLowerCase().contains("iteration")) {
							vConfig.setStartIteration(lastCheckpoint);
							vConfig.setIterationRetry(retries);
							if(refinedRecovery) {
								vConfig.setRefinedRecoveryEnd(currentIteration - 1);
								vConfig.setRefinedRecoveryStart(this.lastCheckpoint);
								vConfig.setRefinedRecoveryOldDop(this.parallelismAtStart);
							}
						}
						//vConfig.setIterationRetry(retries);
					}
					System.out.println("adjusted parallelsim and initializeOnMaster");
					
					// adjust state of this iteration manager
					this.parallelism = this.parallelismAtStart - deadInstances.size();
					this.numberOfEventsUntilEndOfSuperstep = this.numberOfTails * this.parallelism;
					this.workers.clear();
					this.currentIteration = this.lastCheckpoint;
					this.workerDoneEventCounter = 0;
					
					System.out.println("submit");
					
					try {
						eg.restartHard(jobGraph.getVerticesSortedTopologicallyFromSources());
					} catch (InvalidProgramException e) {                                                                                                                                                                                                                                                   
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (JobException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					//jobManager.submitJob(jobGraph, false);
				
				}
				// Iteration over BC variable
				else {
					
					this.retries++;
					
					// adjust parallelism
					for(AbstractJobVertex v : jobGraph.getVertices()) {
						
						// dont change all reduces
						if(v.getParallelism() == this.parallelism && this.parallelism > 1) {
							v.setParallelism(v.getParallelism() - deadInstances.size());
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
							iterationTaskConfig.setIterationRetry(retries);
						}
					}
					
					// adjust state of this iteration manager
					this.parallelism -= deadInstances.size();
					this.numberOfEventsUntilEndOfSuperstep = this.numberOfTails * this.parallelism;
					this.workers.clear();
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
					
					future(new Callable<Object>() {
						@Override
						public Object call() throws Exception {
							try{
								Thread.sleep(2000);
							}catch(InterruptedException e){
								// should only happen on shutdown
							}
							jobManager.currentJobs().get(jobId).get()._1.restartHard(jobGraph.getVerticesSortedTopologicallyFromSources());
							return null;
						}
					}, AkkaUtils.globalExecutionContext());
				}
				
			} catch (Exception e) {
				System.out.println("Something went wrong during recovery");
				System.out.println(e);
				System.out.println(e.getMessage());
				e.printStackTrace(System.out);
				e.printStackTrace();
				throw new RuntimeException("Something went wrong during recovery");
			}
			initRerun.set(false);
		}
	}

	private static <X> InputFormatVertex createCheckpointInput(JobGraph jobGraph, String checkpointPath, 
			int numSubTasks, TypeSerializerFactory<?> serializer, TypeInformation<X> typeInfo, ShipStrategyType shipStrategy,
			TypeComparatorFactory<?> comparator, DataDistribution dataDistribution, Partitioner<?> partitioner) {
		
		System.out.println("createCheckpointInput");
		
		CsvInputFormat<X> pointsInFormat = new CsvInputFormat<X>(new Path(checkpointPath), typeInfo);
		InputFormatVertex pointsInput = createInput(pointsInFormat, checkpointPath, "[CHECKPOINT] path: "+checkpointPath, jobGraph, numSubTasks);
		
		System.out.println("createdInput");
		{
			TaskConfig taskConfig = new TaskConfig(pointsInput.getConfiguration());
			taskConfig.addOutputShipStrategy(shipStrategy);
			taskConfig.setOutputSerializer(serializer);
			
			System.out.println("setOutputComparator");

			if(comparator != null) {
				taskConfig.setOutputComparator(comparator, 0);
			}
			
			System.out.println("setOutputDataDistribution");
			if(dataDistribution != null) {
				taskConfig.setOutputDataDistribution(dataDistribution, 0);
			}
			
			System.out.println("setOutputPartitioner");
			if(partitioner != null) {
				taskConfig.setOutputPartitioner(partitioner, 0);
			}
		}
		return pointsInput;
	}
	
	// from jobgraphutil
	public static <T extends FileInputFormat<?>> InputFormatVertex createInput(T stub, String path, String name, JobGraph graph,
			int parallelism)
	{
		System.out.println("createInput");
		stub.setFilePath(path);
		return createInput(new UserCodeObjectWrapper<T>(stub), name, graph, parallelism);
	}

	private static <T extends InputFormat<?,?>> InputFormatVertex createInput(UserCodeWrapper<T> stub, String name, JobGraph graph,
			int parallelism)
	{
		System.out.println("createInput2");
		InputFormatVertex inputVertex = new InputFormatVertex(name);
		
		inputVertex.setInvokableClass(DataSourceTask.class);
		inputVertex.setParallelism(parallelism);

		TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());
		inputConfig.setStubWrapper(stub);
		
		System.out.println("return inputVertex");
		return inputVertex;
	}
	
	private void removePredecessors(AbstractJobVertex vertex, IntermediateDataSet toRemove) {
//		if(vertex.getSlotSharingGroup() != null) {
//			vertex.getSlotSharingGroup().clearTaskAssignment();
//			vertex.getSlotSharingGroup().removeVertexFromGroup(vertex.getID());
//		}
		if(vertex.getNumberOfProducedIntermediateDataSets() == 1 || toRemove == null) {
			jobGraph.removeVertex(vertex);
			
			if(vertex.getInputs() != null && vertex.getInputs().size() > 0) {
				for(JobEdge edge : vertex.getInputs()) {
					removePredecessors(edge.getSource().getProducer(), null);
				}
			}
		}
		else {
			vertex.getProducedDataSets().remove(toRemove);
			TaskConfig tc = new TaskConfig(vertex.getConfiguration());
			if(tc.getNumberOfChainedStubs() > 0) {
				TaskConfig tcs = tc.getChainedStubConfig(tc.getNumberOfChainedStubs() - 1);
				tcs.setNumOutputs(
						tcs.getNumOutputs() - 1
						);
			}
			else {
				tc.setNumOutputs(
						tc.getNumOutputs() - 1
						);
			}
		}
	}
}