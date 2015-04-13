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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.FileOutputFormat.IterationWriteMode;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.Path;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.Assert;


@SuppressWarnings("serial")
public class BulkIterationWithSinkITCase extends JavaProgramTestBase {

	protected String resultPath;
	protected String resultPath2;
	protected String resultPath3;
	
	private String result1 = "1\n2\n3\n4\n5\n6\n7\n8";
	private String result6 = "6\n7\n8\n9\n10\n11\n12\n13";
	private String result10= "10\n11\n12\n13\n14\n15\n16\n17";
	private String result100= "110\n111\n112\n113\n114\n115\n116\n117";
	
	protected boolean skipCollectionExecution() {
		return true;
	};

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempFilePath("results");
		resultPath2 = getTempFilePath("results2")+"/collect"; // take sub folder for correct clean up
		resultPath3 = getTempFilePath("results3");
	}
	
	@Override
	protected void testProgram() throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		
		DataSet<Tuple1<Integer>> data = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8).map(new MapFunction<Integer, Tuple1<Integer>>() {

			@Override
			public Tuple1<Integer> map(Integer value) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple1<Integer>(value);
			}
		});
		
		IterativeDataSet<Tuple1<Integer>> iteration = data.iterate(10);
		
		iteration.writeAsCsv(resultPath); // by standard iteration write mode is set to OVERWRITE
		iteration.map(new AddHundredMapper()).writeAsCsv(resultPath3);
		
		FileOutputFormat<Tuple1<Integer>> outputFormat = new CsvOutputFormat<Tuple1<Integer>>(new Path(resultPath2), CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER);
		outputFormat.setIterationWriteMode(IterationWriteMode.KEEP_ALL);
		iteration.output(outputFormat);
		
		DataSet<Tuple1<Integer>> result = iteration.map(new AddOneMapper());
		
		final List<Tuple1<Integer>> resultList = new ArrayList<Tuple1<Integer>>();
		iteration.closeWith(result).aggregate(Aggregations.SUM, 0).output(new LocalCollectionOutputFormat<Tuple1<Integer>>(resultList));
		
		env.execute();
		
		Assert.assertEquals(116, resultList.get(0).f0.intValue());
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(result10, resultPath);
		compareResultsByLinesInMemory(result10, resultPath2+"_10");
		compareResultsByLinesInMemory(result6, resultPath2+"_6");
		compareResultsByLinesInMemory(result1, resultPath2+"_1");
		compareResultsByLinesInMemory(result100, resultPath3);
	}

	public static class AddOneMapper implements MapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		
		@Override
		public Tuple1<Integer> map(Tuple1<Integer> record) {
			record.f0 ++;
			return record;
		}
	}
	
	public static class AddHundredMapper implements MapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		
		@Override
		public Tuple1<Integer> map(Tuple1<Integer> record) {
			record.f0 += 100;
			return record;
		}
	}
}
