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


package org.apache.flink.runtime.io.disk;

import java.io.EOFException;
import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.util.MutableObjectIterator;

public class InputViewIteratorCombiner<E> implements MutableObjectIterator<E>
{
	private DataInputView inputView;
	
	private DataInputView inputView2;

	private final TypeSerializer<E> serializer;

	public InputViewIteratorCombiner(DataInputView inputView, DataInputView inputView2, TypeSerializer<E> serializer) {
		this.inputView = inputView;
		this.inputView2 = inputView2;
		this.serializer = serializer;
	}

	@Override
	public E next(E reuse) throws IOException {
		try {
			E t = this.serializer.deserialize(reuse, this.inputView);
			//System.out.println(t);
			return t;
		} catch (EOFException e) {
			if(this.inputView2 != null) {
				System.out.println("SWITCH INPUT VIEW");
				this.inputView = this.inputView2;
				this.inputView2 = null;
				System.out.println();
				try {
					return this.serializer.deserialize(reuse, this.inputView);
				} catch (EOFException e2) {
					System.out.println("FAILFAIL");
					return null;
				}
			}
			else {
				System.out.println("DONE");
				return null;
			}
		}
	}

	@Override
	public E next() throws IOException {
		try {
			return this.serializer.deserialize(this.inputView);
		} catch (EOFException e) {
			if(this.inputView2 != null) {
				this.inputView = this.inputView2;
				this.inputView2 = null;
				try {
					return this.serializer.deserialize(this.inputView);
				} catch (EOFException e2) {
					return null;
				}
			}
			else {
				return null;
			}
		}
	}
}
