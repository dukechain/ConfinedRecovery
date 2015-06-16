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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.util.MutableObjectIterator;

public class InputViewIteratorCombiner<E> implements MutableObjectIterator<E>
{
	private DataInputView inputView;
	
	private DataInputView inputView2;

	private final TypeSerializer<E> serializer;
	
	private TypeComparator<E> comp;
	
	private int i = 0;
	
	private E next1 = null;
	private E next2 = null;

	public InputViewIteratorCombiner(DataInputView inputView, DataInputView inputView2, TypeSerializer<E> serializer, TypeComparator<E> comp) {
		this.inputView = inputView;
		this.inputView2 = inputView2;
		this.serializer = serializer;
		this.comp = comp;
		try {
			if(inputView != null) {
				next1 = this.serializer.deserialize(this.inputView);
			}
			if(inputView2 != null) {
				next2 = this.serializer.deserialize(this.inputView2);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public E next(E reuse) throws IOException {
		if(comp == null) {
			if(next1 != null) {
				E t = next1;
				next1 = null;
				return t;
			}
			if(next2 != null) {
				E t = next2;
				next2 = null;
				return t;
			}
			return next2(reuse);
		}
		
		if(next1 != null && (next2 == null || (comp.compare(next1, next2) < 0))) {
			//System.out.println("COMP1 "+next1+" "+next2);
			
			E t = next1;
			
			try {
				next1 = this.serializer.deserialize(reuse, this.inputView);
			}
			catch (EOFException e) {
				next1 = null;
			}	
			return t;
		}
		else {
			//System.out.println("COMP2 "+next1+" "+next2);
			E t = next2;
			try {
				next2 = this.serializer.deserialize(reuse, this.inputView2);
			}
			catch (EOFException e) {
				next2 = null;
			}
			return t;
		}
	}

	public E next2(E reuse) throws IOException {
		try {
			E t = this.serializer.deserialize(reuse, this.inputView);
			//System.out.println("T "+t);
			i++;
			return t;
		} catch (EOFException e) {
			if(this.inputView2 != null) {
				this.inputView = this.inputView2;
				this.inputView2 = null;
				System.out.println("SWITCH "+i);
				try {
					return this.serializer.deserialize(reuse, this.inputView);
				} catch (EOFException e2) {
					System.out.println("END "+i);
					return null;
				}
			}
			else {
				System.out.println("END "+i);
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
