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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.test.util.JavaProgramTestBase;


@SuppressWarnings("serial")
public class BulkIterationCheckpointingTest extends JavaProgramTestBase {

	protected String resultPath;

	
	protected boolean skipCollectionExecution() {
		return true;
	};

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempFilePath("results");
	}
	
	@Override
	protected void testProgram() throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		
		DataSet<Tuple1<Integer>> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8).map(new MapFunction<Integer, Tuple1<Integer>>() {
			@Override
			public Tuple1<Integer> map(Integer value) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple1<Integer>(value);
			}
		});
		
		IterativeDataSet<Tuple1<Integer>> iteration = data.iterate(10);
		
		iteration.setCheckpointInterval(1);
		
		DataSet<Tuple1<Integer>> result = iteration.map(new AddHundredMapper());
		
		iteration.closeWith(result).aggregate(Aggregations.SUM, 0).print();
		
		env.execute();
		System.out.println("FINISH");
	}
	
	@Override
	protected void postSubmit() throws Exception {
	}

	public static class AddOneMapper implements MapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		
		@Override
		public Tuple1<Integer> map(Tuple1<Integer> record) {
			record.f0 ++;
			return record;
		}
	}
	
	public static class AddHundredMapper extends RichMapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		
		@Override
		public Tuple1<Integer> map(Tuple1<Integer> record) {
			
			System.out.println(getIterationRuntimeContext().getSuperstepNumber());
			if(getIterationRuntimeContext().getSuperstepNumber() == 7) {
				try {
					Thread.sleep(12000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			record.f0 += 100;
			return record;
		}
	}
}
