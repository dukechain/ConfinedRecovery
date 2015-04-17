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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.LabelPropagation;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

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
public class LabelPropagationCheckpointingExample implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// Set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Set up the graph
		DataSet<Vertex<Long, Long>> vertices = getVertexDataSet(env);
		DataSet<Edge<Long, NullValue>> edges = getEdgeDataSet(env);

		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(vertices, edges,	env);

		// Set up the program
		DataSet<Vertex<Long, Long>> verticesWithCommunity = graph.run(
				new LabelPropagation<Long>(maxIterations)).getVertices();

		// Emit results
		if(fileOutput) {
			verticesWithCommunity.writeAsCsv(outputPath, "\n", ",");
		} else {
			verticesWithCommunity.print();
		}

		// Execute the program
		env.execute("Label Propagation Example");
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = true;
	private static String vertexInputPath = null;
	private static String edgeInputPath =  "file:/c:/workspace/dblp.txt";;
	private static String outputPath = "file:/c:/workspace/result.txt";
	private static long numVertices = 517080;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			
			fileOutput = true;
			numVertices = Long.parseLong(args[0]);
			edgeInputPath = args[1];
			outputPath = args[2];
			maxIterations = Integer.parseInt(args[3]);
			vertexInputPath = args[4];
		} else {
			System.out.println("Executing LabelPropagation example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: LabelPropagation <vertex path> <edge path> <output path> <num iterations>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Long>> getVertexDataSet(ExecutionEnvironment env) {

		if (fileOutput && !vertexInputPath.contains("none")) {
			return env.readCsvFile(vertexInputPath)
					.fieldDelimiter(",")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new Tuple2ToVertexMap<Long, Long>());
		}

		return env.generateSequence(1, numVertices).map(
				new MapFunction<Long, Vertex<Long, Long>>() {
					public Vertex<Long, Long> map(Long l) throws Exception {
						return new Vertex<Long, Long>(l, l);
					}
				});
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgeDataSet(ExecutionEnvironment env) {

		if (fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.ignoreComments("#")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
						@Override
						public Edge<Long, NullValue> map(Tuple2<Long, Long> value) throws Exception {
							return new Edge<Long, NullValue>(value.f0, value.f1, NullValue.getInstance());
						}
					});
		}

		return env.generateSequence(1, numVertices).flatMap(
				new FlatMapFunction<Long, Edge<Long, NullValue>>() {
					@Override
					public void flatMap(Long key,
							Collector<Edge<Long, NullValue>> out) {
						int numOutEdges = (int) (Math.random() * (numVertices / 2));
						for (int i = 0; i < numOutEdges; i++) {
							long target = (long) (Math.random() * numVertices) + 1;
							out.collect(new Edge<Long, NullValue>(key, target,
									NullValue.getInstance()));
						}
					}
				});
	}

	@Override
	public String getDescription() {
		return "Label Propagation Example";
	}
}