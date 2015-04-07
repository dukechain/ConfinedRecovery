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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.util.IdentityMap;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;


@SuppressWarnings("serial")
public class BulkIterationWithAllReducerITCaseSinks extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(2);
		
		DataSet<Tuple1<Integer>> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8).map(new MapFunction<Integer, Tuple1<Integer>>() {

			@Override
			public Tuple1<Integer> map(Integer value) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple1<Integer>(value);
			}
		});
		
		IterativeDataSet<Tuple1<Integer>> iteration = data.iterate(10);
		
		iteration.writeAsCsv("file:/C:/temp/test");
		
		DataSet<Tuple1<Integer>> result = data.reduceGroup(new PickOneAllReduce()).withBroadcastSet(iteration, "bc");
		
		final List<Tuple1<Integer>> resultList = new ArrayList<Tuple1<Integer>>();
		iteration.closeWith(result).output(new LocalCollectionOutputFormat<Tuple1<Integer>>(resultList));
		
		env.execute();
		
		Assert.assertEquals(8, resultList.get(0).f0.intValue());
	}

	
	public static class PickOneAllReduce extends RichGroupReduceFunction<Tuple1<Integer>, Tuple1<Integer>> {
		
		private Tuple1<Integer> bcValue;
		
		@Override
		public void open(Configuration parameters) {
			Collection<Tuple1<Integer>> bc = getRuntimeContext().getBroadcastVariable("bc");
			synchronized (bc) {
				this.bcValue = bc.isEmpty() ? null : bc.iterator().next();
			}
		}

		@Override
		public void reduce(Iterable<Tuple1<Integer>> records, Collector<Tuple1<Integer>> out) {
			System.out.println("reduce");
			
			if (bcValue == null) {
				return;
			}
			final int x = bcValue.f0;
			
			for (Tuple1<Integer> y : records) { 
				if (y.f0 > x) {
					out.collect(y);
					return;
				}
			}

			out.collect(bcValue);
		}
	}
}
