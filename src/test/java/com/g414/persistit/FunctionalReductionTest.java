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
import com.g414.persistit.Functional.Pair;
import com.g414.persistit.Functional.Reduction;
import com.g414.persistit.Functional.TraversalSpec;
import com.persistit.Exchange;
import com.persistit.KeyFilter;
import com.persistit.KeyFilter.Term;

@Test
public class FunctionalReductionTest extends FunctionalTestBase {
	/**
	 * Tests Functional#reduce with the summation reduction; we traverse all
	 * keys (in each direction) to make sure the sum and count are correct and
	 * keys and values match up.
	 */
	public void testSummationReduction() throws Exception {
		final AtomicLong counter = new AtomicLong();
		final Exchange exchange = getExchange(db, true);

		Reduction<String, Integer, Integer> summation = new Reduction<String, Integer, Integer>() {
			@Override
			public Integer reduce(Pair<String, Integer> row, Integer accum) {
				Assert.assertEquals(row.getKey(), getKey(row.getValue()));
				counter.getAndIncrement();

				return accum + row.getValue();
			}
		};

		int ascendingSum = Functional.reduce(exchange,
				getFullTraversal(Direction.ASC), summation, 0);
		Assert.assertEquals(ascendingSum, 499500);
		Assert.assertEquals(counter.get(), 1000);

		counter.set(0);

		int descendingSum = Functional.reduce(exchange,
				getFullTraversal(Direction.DESC), summation, 0);

		Assert.assertEquals(descendingSum, 499500);
		Assert.assertEquals(counter.get(), 1000);
	}

	/**
	 * Tests Functional#reduce with the summation reduction and a row filter (a
	 * user-defined filter that is not a KeyFilter); we traverse all keys (in
	 * each direction) to make sure the sum and count are correct and keys and
	 * values match up.
	 */
	public void testSummationReductionWithRowFilter() throws Exception {
		final AtomicLong counter = new AtomicLong();
		final Exchange exchange = getExchange(db, true);

		Reduction<String, Integer, Integer> summation = new Reduction<String, Integer, Integer>() {
			@Override
			public Integer reduce(Pair<String, Integer> row, Integer accum) {
				Assert.assertEquals(row.getKey(), getKey(row.getValue()));
				counter.getAndIncrement();

				return accum + row.getValue();
			}
		};

		int ascendingSum = Functional.reduce(
				exchange,
				getFilteredTraversal(Direction.ASC,
						new Filter<String, Integer>() {
							@Override
							public Boolean map(Pair<String, Integer> row) {
								return row.getValue() % 2 == 0;
							}
						}), summation, 0);
		Assert.assertEquals(ascendingSum, 249500);
		Assert.assertEquals(counter.get(), 500);

		counter.set(0);

		int descendingSum = Functional.reduce(
				exchange,
				getFilteredTraversal(Direction.DESC,
						new Filter<String, Integer>() {
							@Override
							public Boolean map(Pair<String, Integer> row) {
								return row.getValue() % 2 == 0;
							}
						}), summation, 0);
		Assert.assertEquals(descendingSum, 249500);
		Assert.assertEquals(counter.get(), 500);
	}

	/**
	 * Tests Functional#reduce with the summation reduction and a KeyFilter; we
	 * traverse the bounded keys (in each direction) to make sure the sum and
	 * count are correct and keys and values match up.
	 */
	public void testSummationReductionWithKeyFilter() throws Exception {
		final AtomicLong counter = new AtomicLong();
		final Exchange exchange = getExchange(db, true);

		Reduction<String, Integer, Integer> summation = new Reduction<String, Integer, Integer>() {
			@Override
			public Integer reduce(Pair<String, Integer> row, Integer accum) {
				Assert.assertEquals(row.getKey(), getKey(row.getValue()));
				counter.getAndIncrement();

				return accum + row.getValue();
			}
		};

		KeyFilter filter100 = new KeyFilter(new Term[] { KeyFilter.rangeTerm(
				getKey(100), getKey(200), true, false) });

		int ascendingSum = Functional.reduce(exchange,
				new TraversalSpec<String, Integer>(Direction.ASC, filter100,
						null), summation, 0);

		Assert.assertEquals(ascendingSum, 14950);
		Assert.assertEquals(counter.get(), 100);

		counter.set(0);

		KeyFilter filter300 = new KeyFilter(new Term[] { KeyFilter.rangeTerm(
				getKey(300), getKey(400), true, false) });

		int descendingSum = Functional.reduce(exchange,
				new TraversalSpec<String, Integer>(Direction.DESC, filter300,
						null), summation, 0);
		Assert.assertEquals(descendingSum, 34950);
		Assert.assertEquals(counter.get(), 100);
	}
}
