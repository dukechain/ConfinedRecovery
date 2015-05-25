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

package org.apache.flink.runtime.io.network.api.writer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.api.common.io.FileOutputFormat.OutputDirectoryMode;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.disk.iomanager.RequestQueue;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.iterative.task.IterationHeadPactTask;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.RecoveryUtil;

/**
 * A record-oriented runtime result writer.
 * <p>
 * The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * serializing records into buffers.
 * <p>
 * <strong>Important</strong>: it is necessary to call {@link #flush()} after
 * all records have been written with {@link #emit(IOReadableWritable)}. This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T extends IOReadableWritable> {

	protected final ResultPartitionWriter writer;

	private final ChannelSelector<T> channelSelector;

	private final int numChannels;
	
	private LogWriterThread[] logOutput = null;

	/** {@link RecordSerializer} per outgoing channel */
	private final RecordSerializer<T>[] serializers;
	
	BufferFileWriter spillWriter;
	
	IOManager ioManager;
	
	int indexInSubtaskGroup;
	int numberOfSubtasks;
	
	TaskConfig config;
	
	int foreignIndex = -1;
	
	boolean doLogging = false;

	public RecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>(), 1, 1, null);
	}
	
	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		this(writer, channelSelector, 1, 1, null);
	}

	@SuppressWarnings("unchecked")
	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, 
			int indexInSubtaskGroup, int numberOfSubtasks, Configuration config) {
		this.writer = writer;
		this.channelSelector = channelSelector;

		this.numChannels = writer.getNumberOfOutputChannels();
		this.indexInSubtaskGroup = indexInSubtaskGroup;
		this.numberOfSubtasks = numberOfSubtasks;
		
		if(writer.getPartition().getOwnQueueToRequest() > 0) {
			foreignIndex = 0;
		}
		else if(writer.getPartition().getOwnQueueToRequest() == 0 && writer.getPartition().getNumberOfSubpartitions() > 1) {
			foreignIndex = 1;
		}
		
		// log outgoing messages in case of refined recovery
		this.doLogging = 
				GlobalConfiguration.getBoolean(ConfigConstants.REFINED_RECOVERY, ConfigConstants.REFINED_RECOVERY_DEFAULT);

		/**
		 * The runtime exposes a channel abstraction for the produced results
		 * (see {@link ChannelSelector}). Every channel has an independent
		 * serializer.
		 */
		this.serializers = new SpanningRecordSerializer[numChannels];
		for (int i = 0; i < numChannels; i++) {
			serializers[i] = new SpanningRecordSerializer<T>();
		}
		
		if(doLogging) {
			logOutput = new LogWriterThread[writer.getPartition().getNumberOfSubpartitions()];
		}
		
		this.ioManager = new IOManagerAsync();
		this.config = new TaskConfig(config);
	}

	public void emit(T record) throws IOException, InterruptedException {
		
		if(doLogging) {
			// during refined recovery only keep records that would have been forwarded locally in the
			// original execution
			if(config.getRefinedRecoveryLostNode() > -1 && 
					IterationHeadPactTask.SUPERSTEP.get() <= config.getRefinedRecoveryEnd() &&
				channelSelector.selectChannels(record, config.getRefinedRecoveryOldDop())[0] != 
						config.getRefinedRecoveryLostNode()) {
				return;
			}

			this.setupLogOutput();
		}
		
		for (int targetChannel : channelSelector.selectChannels(record, numChannels)) {
			
			if(doLogging) {
				// log outgoing messages for refined recovery
				if(logOutput != null && writer.getPartition().getNumberOfSubpartitions() > 1 
						&& IterationHeadPactTask.SUPERSTEP.get() > -1 && writer.getPartition().getOwnQueueToRequest() != targetChannel
						&& writer.getPartition().getOwnQueueToRequest() != -1) {
					if(record instanceof SerializationDelegate) {
						SerializationDelegate<T> sd = (SerializationDelegate<T>) record;
						if(sd.getInstance() instanceof Tuple) {
							logOutput[targetChannel].requestQueue.add((Tuple) sd.getInstance());//.writeRecord((Tuple) sd.getInstance());
							//logOutput[targetChannel].output.writeRecord((Tuple) sd.getInstance());
						}
					}
				}
			}
			
			// serialize with corresponding serializer and send full buffer
			RecordSerializer<T> serializer = serializers[targetChannel];

			synchronized (serializer) {
				SerializationResult result = serializer.addRecord(record);
				while (result.isFullBuffer()) {
					Buffer buffer = serializer.getCurrentBuffer();

					if (buffer != null) {
						writer.writeBuffer(buffer, targetChannel);
					}

					buffer = writer.getBufferProvider().requestBufferBlocking();
					result = serializer.setNextBuffer(buffer);
				}
			}
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			RecordSerializer<T> serializer = serializers[targetChannel];

			synchronized (serializer) {

				if (serializer.hasData()) {
					Buffer buffer = serializer.getCurrentBuffer();
					if (buffer == null) {
						throw new IllegalStateException("Serializer has data but no buffer.");
					}

					writer.writeBuffer(buffer, targetChannel);
					writer.writeEvent(event, targetChannel);

					buffer = writer.getBufferProvider().requestBufferBlocking();
					serializer.setNextBuffer(buffer);
				}
				else {
					writer.writeEvent(event, targetChannel);
				}
			}
		}
	}

	public void sendEndOfSuperstep() throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			RecordSerializer<T> serializer = serializers[targetChannel];

			synchronized (serializer) {
				Buffer buffer = serializer.getCurrentBuffer();
				if (buffer != null) {

					writer.writeBuffer(buffer, targetChannel);

					buffer = writer.getBufferProvider().requestBufferBlocking();
					serializer.setNextBuffer(buffer);
				}
			}
		}

		writer.writeEndOfSuperstep();
	}

	public void flush() throws IOException {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			RecordSerializer<T> serializer = serializers[targetChannel];

			synchronized (serializer) {
				Buffer buffer = serializer.getCurrentBuffer();
				serializer.clear();

				if (buffer != null) {
					writer.writeBuffer(buffer, targetChannel);
				}
			}
		}
		
		for(int i = 0; i < writer.getPartition().getNumberOfSubpartitions(); i++) {
			if(logOutput != null && logOutput[i] != null) {
				// with this call we make sure all logs are written until we continue
				logOutput[i].shutdownGrace();
			}
		}
	}

	public void clearBuffers() {
		if (serializers != null) {
			for (RecordSerializer<?> s : serializers) {
				Buffer b = s.getCurrentBuffer();
				if (b != null && !b.isRecycled()) {
					b.recycle();
				}
			}
		}
	}
	
	/**
	 * Initializes CsvOutputFormats for logging, used for refined recovery
	 */
	private void setupLogOutput() {
		
		// either first logOutput has to be initialized
		// or it has to be re-initialized for next superstep
		if(foreignIndex != -1 && ((logOutput[foreignIndex] == null && writer.getPartition().getNumberOfSubpartitions() > 1 
				&& IterationHeadPactTask.SUPERSTEP.get() > -1) || (logOutput[foreignIndex] != null 
				&& !logOutput[foreignIndex].getOutput().getOutputFilePath().toString().endsWith("_"+IterationHeadPactTask.SUPERSTEP.get())))) {
			
			// for all outgoing partitions
			for(int i = 0; i < writer.getPartition().getNumberOfSubpartitions(); i++) {
				
				// make sure we dont log our own messages
				if(writer.getPartition().getOwnQueueToRequest() != -1 &&
						writer.getPartition().getOwnQueueToRequest() != i) {

					String logPath = RecoveryUtil.getLoggingPath();
					logPath += "/flinklog_"+writer.getIntermediateDataSetID()+"_"+i+"_"+IterationHeadPactTask.SUPERSTEP.get();
					
					CsvOutputFormat<Tuple> csvOut = new CsvOutputFormat<Tuple>(new Path(logPath));
					csvOut.setWriteMode(WriteMode.OVERWRITE);
					csvOut.setOutputDirectoryMode(OutputDirectoryMode.PARONLY);
					try {
						csvOut.open(indexInSubtaskGroup, numberOfSubtasks);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					logOutput[i] = new LogWriterThread(csvOut);
					logOutput[i].start();
				}
			}
		}
	}
	
	/**
	 * A worker thread that asynchronously writes the buffers to disk.
	 */
	private static final class LogWriterThread extends Thread {
		
		protected final RequestQueue<Tuple> requestQueue;
		
		protected final CsvOutputFormat<Tuple> output;

		public CsvOutputFormat<Tuple> getOutput() {
			return output;
		}

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------

		protected LogWriterThread(CsvOutputFormat<Tuple> output) {
			this.requestQueue = new RequestQueue<Tuple>();
			this.alive = true;
			this.output = output;
		}

		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel writers and an exception is
		 * reported to them, declaring their request queue as closed.
		 */
		protected void shutdownGrace() {
			//synchronized (this) {
			
				//ArrayList<Tuple> req = new ArrayList<Tuple>();
				if (alive) {
					alive = false;
					requestQueue.close();
				}
				
//				if(! this.requestQueue.isEmpty()) {
//					requestQueue.drainTo(req);
//				}
				
//				try {
//					for(Tuple request : req) {
//						// write buffer to the specified channel
//						output.writeRecord((Tuple) request);
//					}
//				}
//				catch (IOException e) {
//					throw new RuntimeException();
//				}
//				req.clear();
				
//				while (!this.requestQueue.isEmpty()) {
//					Tuple request = this.requestQueue.poll();
//					if (request != null) {
//						try {
//							output.writeRecord((Tuple) request);
//						} catch (IOException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//					}
//				}
			//}
		}

		// ---------------------------------------------------------------------
		// Main loop
		// ---------------------------------------------------------------------

		@Override
		public void run() {
			
			ArrayList<Tuple> req = new ArrayList<Tuple>();
			
			while (this.alive) {
				
				//Tuple request = null;
				//ArrayList<Tuple> req = new ArrayList<Tuple>();
				
				// get the next buffer. ignore interrupts that are not due to a shutdown.
//				try {
//					req.add(requestQueue.take());
//					requestQueue.drainTo(req);
//				}
//				catch (InterruptedException e) {
//					if (!this.alive) {
//						return;
//					}
//				}
				
				requestQueue.drainTo(req);

				try {
					for(Tuple request : req) {
						// write buffer to the specified channel
						output.writeRecord((Tuple) request);
					}
				}
				catch (IOException e) {
					throw new RuntimeException();
				}
				req.clear();
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} // end while alive
			
			// make sure everything is written
			if(! this.requestQueue.isEmpty()) {
				requestQueue.drainTo(req);
			}
		
			try {
				for(Tuple request : req) {
					// write buffer to the specified channel
					output.writeRecord((Tuple) request);
				}
			}
			catch (IOException e) {
				throw new RuntimeException();
			}
			req.clear();
		}
		
	}; // end writer thread
}
