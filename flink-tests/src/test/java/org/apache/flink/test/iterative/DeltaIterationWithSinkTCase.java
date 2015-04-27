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

package org.apache.flink.test.iterative;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.JavaProgramTestBase;

@SuppressWarnings("serial")
public class DeltaIterationWithSinkTCase extends JavaProgramTestBase {
	private final List<Tuple2<Long, Long>> result = new ArrayList<Tuple2<Long,Long>>();
	
	protected String resultPath;
	protected String resultPath2;
	
	private String result1 = "40,40\n41,41\n42,42\n43,43\n44,44\n45,45\n46,46\n47,47\n48,48\n49,49";
	private String result2 = "150\n151\n152\n153\n154\n155\n156\n157\n158\n159";	
	
	protected boolean skipCollectionExecution() {
		return true;
	};

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempFilePath("results");
		resultPath2 = getTempFilePath("results2");
	}
	
	@Override
	protected void testProgram() throws Exception {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(1);

			DataSet<Tuple2<Long, Long>> input = env.generateSequence(0, 9).map(new Duplicator<Long>());

			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = input.iterateDelta(input, 5, 1);

			iteration.getWorkset().writeAsCsv(resultPath);
			
			DataSet<Tuple2<Long, Long>> newWorkset =  iteration.getWorkset().map(new TestMapper());
			
			newWorkset.map(new AddHundredMapper()).writeAsCsv(resultPath2);
			
			iteration.closeWith(iteration.getWorkset(), newWorkset)
					.output(new LocalCollectionOutputFormat<Tuple2<Long,Long>>(result));

			env.execute();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Override
	protected void postSubmit() throws Exception {
		boolean[] present = new boolean[50];
		for (Tuple2<Long, Long> t : result) {
			present[t.f0.intValue()] = true;
		}

		for (int i = 0; i < present.length; i++) {
			assertTrue(String.format("Missing tuple (%d, %d)", i, i), present[i]);
		}
		
		compareResultsByLinesInMemory(result1, resultPath);
		compareResultsByLinesInMemory(result2, resultPath2);
	}

	private static final class Duplicator<T> implements MapFunction<T, Tuple2<T, T>> {
		@Override
		public Tuple2<T, T> map(T value) {
			return new Tuple2<T, T>(value, value);
		}
	}
	
	private static final class TestMapper extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Long> value) {
			return new Tuple2<Long, Long>(value.f0+10, value.f1+10);
		}
	}
	
	public static class AddHundredMapper implements MapFunction<Tuple2<Long, Long>, Tuple1<Long>> {
		
		@Override
		public Tuple1<Long> map(Tuple2<Long, Long> record) {
			return new Tuple1<Long>(record.f0 + 100);
		}
	}
}
