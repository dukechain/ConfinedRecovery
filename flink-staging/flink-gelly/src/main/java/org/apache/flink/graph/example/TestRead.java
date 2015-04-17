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

package org.apache.flink.graph.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * This example uses the label propagation algorithm to detect communities by
 * propagating labels. Initially, each vertex is assigned its id as its label.
 * The vertices iteratively propagate their labels to their neighbors and adopt
 * the most frequent label among their neighbors. The algorithm converges when
 * no vertex changes value or the maximum number of iterations have been
 * reached.
 *
 * The edges input file is expected to contain one edge per line, with long IDs
 * in the following format:"<sourceVertexID>\t<targetVertexID>".
 *
 * The vertices input file is expected to contain one vertex per line, with long IDs
 * and long vertex values, in the following format:"<vertexID>\t<vertexValue>".
 *
 * If no arguments are provided, the example runs with a random graph of 100 vertices.
 */
public class TestRead implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		// Set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		env.setParallelism(2);

		String inputPath = args[0];
		
		env.readCsvFile(inputPath).types(Long.class).print();

		// Execute the program
		env.execute("Test Write");
	}

	@Override
	public String getDescription() {
		return "Label Propagation Example";
	}
}