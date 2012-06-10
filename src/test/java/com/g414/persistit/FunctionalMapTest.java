/**
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
package com.g414.persistit;

import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.g414.persistit.Functional.Direction;
import com.g414.persistit.Functional.Filter;
import com.g414.persistit.Functional.Mapping;
import com.g414.persistit.Functional.Pair;
import com.g414.persistit.Functional.Traversal;
import com.g414.persistit.Functional.TraversalSpec;
import com.persistit.KeyFilter;
import com.persistit.KeyFilter.Term;

@Test
public class FunctionalMapTest extends FunctionalTestBase {
	/**
	 * Tests Functional#map with the identity mapping (Integer to Integer); we
	 * traverse all keys (in each direction) to make sure the count is correct
	 * and keys and values match up.
	 */
	public void testIdentityMapping() throws Exception {
		final AtomicLong counter = new AtomicLong();

		Mapping<String, Integer, Integer> identity = new Mapping<String, Integer, Integer>() {
			@Override
			public Integer map(Pair<String, Integer> row) {
				Assert.assertEquals(row.getKey(), getKey(row.getValue()));
				counter.getAndIncrement();

				return row.getValue();
			}
		};

		Traversal<String, Integer, Integer> traversalAscending = Functional
				.map(getFullTraversal(db, Direction.ASC), identity);

		while (traversalAscending.hasNext()) {
			Integer value = traversalAscending.next();
			Assert.assertEquals((int) counter.get() - 1, value.intValue());
		}

		Assert.assertEquals(counter.get(), 1000);

		counter.set(0);

		Traversal<String, Integer, Integer> traversalDescending = Functional
				.map(getFullTraversal(db, Direction.DESC), identity);

		while (traversalDescending.hasNext()) {
			Integer value = traversalDescending.next();
			Assert.assertEquals(1000 - (int) counter.get(), value.intValue());
		}

		Assert.assertEquals(counter.get(), 1000);
	}

	/**
	 * Tests Functional#map with the identity mapping (Integer to Integer) and a
	 * row filter (a user-defined filter that is not a KeyFilter); we traverse
	 * all keys (in each direction) to make sure the filtered count is correct
	 * and filtered keys and values match up.
	 */
	public void testIdentityMappingWithRowFilter() throws Exception {
		final AtomicLong counter = new AtomicLong();

		Mapping<String, Integer, Integer> identity = new Mapping<String, Integer, Integer>() {
			@Override
			public Integer map(Pair<String, Integer> row) {
				Assert.assertEquals(row.getKey(), getKey(row.getValue()));
				counter.getAndIncrement();

				return row.getValue();
			}
		};

		Traversal<String, Integer, Integer> traversalAscending = Functional
				.map(getFilteredTraversal(db, Direction.ASC,
						new Filter<String, Integer>() {
							@Override
							public Boolean map(Pair<String, Integer> row) {
								return row.getValue() % 2 == 0;
							}
						}), identity);

		while (traversalAscending.hasNext()) {
			Integer value = traversalAscending.next();
			Assert.assertEquals(((int) counter.get() - 1) * 2, value.intValue());
		}

		Assert.assertEquals(counter.get(), 500);

		counter.set(0);

		Traversal<String, Integer, Integer> traversalDescending = Functional
				.map(getFilteredTraversal(db, Direction.DESC,
						new Filter<String, Integer>() {
							@Override
							public Boolean map(Pair<String, Integer> row) {
								return row.getValue() % 2 == 0;
							}
						}), identity);

		while (traversalDescending.hasNext()) {
			Integer value = traversalDescending.next();
			Assert.assertEquals((1000 - ((int) counter.get() * 2)),
					value.intValue());
		}

		Assert.assertEquals(counter.get(), 500);
	}

	/**
	 * Tests Functional#map with the identity mapping (Integer to Integer) and a
	 * persistit KeyFilter; we traverse the key range (in each direction) to
	 * make sure the filtered count is correct and filtered keys and values
	 * match up.
	 */
	public void testIdentityMappingWithKeyFilter() throws Exception {
		final AtomicLong counter = new AtomicLong();

		Mapping<String, Integer, Integer> identity = new Mapping<String, Integer, Integer>() {
			@Override
			public Integer map(Pair<String, Integer> row) {
				Assert.assertEquals(row.getKey(), getKey(row.getValue()));
				counter.getAndIncrement();

				return row.getValue();
			}
		};

		KeyFilter filter100 = new KeyFilter(new Term[] { KeyFilter.rangeTerm(
				getKey(100), getKey(200), true, false) });

		Traversal<String, Integer, Integer> traversalAscending = Functional
				.map(new TraversalSpec<String, Integer>(db.getExchange(vol,
						tree, false), Direction.ASC, filter100, null), identity);

		while (traversalAscending.hasNext()) {
			Integer value = traversalAscending.next();
			Assert.assertEquals(100 + ((int) counter.get() - 1),
					value.intValue());
		}

		Assert.assertEquals(100, counter.get());

		counter.set(0);

		KeyFilter filter300 = new KeyFilter(new Term[] { KeyFilter.rangeTerm(
				getKey(300), getKey(400), true, false) });

		Traversal<String, Integer, Integer> traversalDescending = Functional
				.map(new TraversalSpec<String, Integer>(db.getExchange(vol,
						tree, false), Direction.DESC, filter300, null),
						identity);

		while (traversalDescending.hasNext()) {
			Integer value = traversalDescending.next();
			Assert.assertEquals((400 - (int) counter.get()), value.intValue());
		}

		Assert.assertEquals(counter.get(), 100);
	}

	/**
	 * Tests Functional#map with a simple mapping (Integer to String) to make
	 * sure the count is correct and mapped keys and values match up.
	 */
	public void testMapIntegersToStrings() throws Exception {
		final AtomicLong counter = new AtomicLong();

		Traversal<String, Integer, String> traversalAscending = Functional.map(
				getFullTraversal(db, Direction.ASC),
				new Mapping<String, Integer, String>() {
					@Override
					public String map(Pair<String, Integer> row) {
						Assert.assertEquals(row.getKey(),
								getKey(row.getValue()));
						counter.getAndIncrement();

						return row.getValue().toString();
					}
				});

		while (traversalAscending.hasNext()) {
			String value = traversalAscending.next();
			Assert.assertEquals(Integer.toString((int) counter.get() - 1),
					value);
		}

		Assert.assertEquals(counter.get(), 1000);
	}
}
