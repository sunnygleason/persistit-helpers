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
import com.g414.persistit.Functional.Mutation;
import com.g414.persistit.Functional.MutationType;
import com.g414.persistit.Functional.Pair;
import com.g414.persistit.Functional.PairImpl;
import com.g414.persistit.Functional.Traversal;
import com.g414.persistit.Template.TransactionCallback;
import com.persistit.Exchange;
import com.persistit.Transaction;

@Test
public class FunctionalApplyTest extends FunctionalTestBase {
	/**
	 * Tests Functional#apply with an increment mutation (Integer to Integer);
	 * we traverse all keys (in each direction) to make sure the count is
	 * correct and keys and values match up.
	 */
	public void testApplyIncrementMutation() throws Exception {
		final AtomicLong counter = new AtomicLong();
		final Exchange exchange = getExchange(db, true);

		Traversal<String, Integer, Integer> traversalAscending = Functional
				.map(exchange, getFullTraversal(Direction.ASC),
						getIdentityMapping(counter, true, true));

		while (traversalAscending.hasNext()) {
			Integer value = traversalAscending.next();
			Assert.assertEquals((int) counter.get() - 1, value.intValue());
		}

		Assert.assertEquals(counter.get(), 1000);

		final Template<String, Integer> template = new Template<String, Integer>();

		final Mapping<String, Integer, Mutation<String, Integer>> incrementMutation = new Mapping<String, Integer, Functional.Mutation<String, Integer>>() {
			@Override
			public Mutation<String, Integer> map(Pair<String, Integer> row) {
				return new Mutation<String, Integer>(
						MutationType.INSERT_OR_UPDATE,
						new PairImpl<String, Integer>(row.getKey(),
								row.getValue() + 1));
			}
		};

		final Exchange source = getExchange(db, true);
		final Exchange target = getExchange(db, true);

		template.inTransaction(db, new TransactionCallback<Void>() {
			@Override
			public Void inTransaction(Transaction txn) {
				Functional.apply(source, template, incrementMutation, target)
						.traverseAll();

				return null;
			}
		});

		counter.set(0);

		Traversal<String, Integer, Integer> traversalDescending = Functional
				.map(exchange, getFullTraversal(Direction.DESC),
						getIdentityMapping(counter, true, false));

		while (traversalDescending.hasNext()) {
			Integer value = traversalDescending.next();
			Assert.assertEquals(1001 - (int) counter.get(), value.intValue());
		}

		Assert.assertEquals(counter.get(), 1000);
	}

	/**
	 * Tests Functional#apply with a decrement mutation (Integer to Integer); we
	 * traverse all keys (in each direction) to make sure the count is correct
	 * and keys and values match up.
	 */
	public void testApplyDecrementMutation() throws Exception {
		final AtomicLong counter = new AtomicLong();
		final Exchange exchange = getExchange(db, true);

		Traversal<String, Integer, Integer> traversalAscending = Functional
				.map(exchange, getFullTraversal(Direction.ASC),
						getIdentityMapping(counter, true, true));

		while (traversalAscending.hasNext()) {
			Integer value = traversalAscending.next();
			Assert.assertEquals((int) counter.get() - 1, value.intValue());
		}

		Assert.assertEquals(counter.get(), 1000);

		final Template<String, Integer> template = new Template<String, Integer>();

		final Mapping<String, Integer, Mutation<String, Integer>> decrementMutation = new Mapping<String, Integer, Functional.Mutation<String, Integer>>() {
			@Override
			public Mutation<String, Integer> map(Pair<String, Integer> row) {
				return new Mutation<String, Integer>(
						MutationType.INSERT_OR_UPDATE,
						new PairImpl<String, Integer>(row.getKey(),
								row.getValue() - 1));
			}
		};

		final Exchange source = getExchange(db, true);
		final Exchange target = getExchange(db, true);

		template.inTransaction(db, new TransactionCallback<Void>() {
			@Override
			public Void inTransaction(Transaction txn) {
				Functional.apply(source, template, decrementMutation, target)
						.traverseAll();

				return null;
			}
		});

		counter.set(0);

		Traversal<String, Integer, Integer> traversalDescending = Functional
				.map(exchange, getFullTraversal(Direction.DESC),
						getIdentityMapping(counter, true, false));

		while (traversalDescending.hasNext()) {
			Integer value = traversalDescending.next();
			Assert.assertEquals(999 - (int) counter.get(), value.intValue());
		}

		Assert.assertEquals(counter.get(), 1000);
	}

	/**
	 * Tests Functional#apply with an insertion mutation (Integer to Integer);
	 * we traverse all keys (in each direction) to make sure the count is
	 * correct and keys and values match up. NOTE: insertion mutations can be
	 * dangerous and cause infinite loops if not bounds-checked!
	 */
	public void testApplyInsertionMutation() throws Exception {
		final AtomicLong counter = new AtomicLong();
		final Exchange first = getExchange(db, true);

		Traversal<String, Integer, Integer> traversalAscending = Functional
				.map(first, getFullTraversal(Direction.ASC),
						getIdentityMapping(counter, true, true));

		while (traversalAscending.hasNext()) {
			Integer value = traversalAscending.next();
			Assert.assertEquals((int) counter.get() - 1, value.intValue());
		}

		db.releaseExchange(first);

		Assert.assertEquals(counter.get(), 1000);

		final Template<String, Integer> template = new Template<String, Integer>();

		final Mapping<String, Integer, Mutation<String, Integer>> insertNewRowPlus1000 = new Mapping<String, Integer, Mutation<String, Integer>>() {
			@Override
			public Mutation<String, Integer> map(Pair<String, Integer> row) {
				Pair<String, Integer> todo = new PairImpl<String, Integer>(
						getKey(row.getValue() + 1000), row.getValue() + 1000);

				return new Mutation<String, Integer>(
						MutationType.INSERT_OR_UPDATE, todo);
			}
		};

		final Exchange source = getExchange(db, true);
		final Exchange target = getExchange(db, true);

		template.inTransaction(db, new TransactionCallback<Void>() {
			@Override
			public Void inTransaction(Transaction txn) {
				Functional
						.apply(source, template, insertNewRowPlus1000, target)
						.traverseAll();

				return null;
			}
		});

		counter.set(0);

		Traversal<String, Integer, Integer> traversalDescending = Functional
				.map(source, getFullTraversal(Direction.DESC),
						getIdentityMapping(counter, true, false));

		while (traversalDescending.hasNext()) {
			Integer value = traversalDescending.next();
			Assert.assertEquals(2000 - (int) counter.get(), value.intValue());
		}

		Assert.assertEquals(counter.get(), 2000);
	}

	/**
	 * Tests Functional#apply with an insertion mutation (Integer to Integer);
	 * we traverse a filtered subset of keys (in each direction) to make sure
	 * the count is correct, keys and values match up, and that we have a
	 * bounded number of insertions.
	 */
	public void testApplyInsertionMutationWithFilter() throws Exception {
		final AtomicLong counter = new AtomicLong();
		final Exchange sourceExchange = getExchange(db, true);

		Traversal<String, Integer, Integer> traversalAscending = Functional
				.map(sourceExchange, getFullTraversal(Direction.ASC),
						getIdentityMapping(counter, true, true));

		while (traversalAscending.hasNext()) {
			Integer value = traversalAscending.next();
			Assert.assertEquals((int) counter.get() - 1, value.intValue());
		}

		Assert.assertEquals(counter.get(), 1000);

		final Template<String, Integer> template = new Template<String, Integer>();

		final Mapping<String, Integer, Mutation<String, Integer>> insertNewRowPlus1000 = new Mapping<String, Integer, Functional.Mutation<String, Integer>>() {
			@Override
			public Mutation<String, Integer> map(Pair<String, Integer> row) {
				Pair<String, Integer> todo = new PairImpl<String, Integer>(
						getKey(row.getValue() + 1000), row.getValue() + 1000);
				return new Mutation<String, Integer>(
						MutationType.INSERT_OR_UPDATE, todo);
			}
		};

		final Exchange source = getExchange(db, true);
		final Exchange target = getExchange(db, true);

		template.inTransaction(db, new TransactionCallback<Void>() {
			@Override
			public Void inTransaction(Transaction txn) {
				Functional.apply(
						source,
						template,
						getFilteredTraversal(Direction.ASC,
								new Filter<String, Integer>() {
									@Override
									public Boolean map(Pair<String, Integer> row) {
										return row.getValue() < 1000;
									}
								}), insertNewRowPlus1000, target).traverseAll();

				return null;
			}
		});

		counter.set(0);

		Traversal<String, Integer, Integer> traversalDescending = Functional
				.map(sourceExchange, getFullTraversal(Direction.DESC),
						getIdentityMapping(counter, true, false));

		while (traversalDescending.hasNext()) {
			Integer value = traversalDescending.next();
			Assert.assertEquals(2000 - (int) counter.get(), value.intValue());
		}

		Assert.assertEquals(counter.get(), 2000);
	}

	/**
	 * Tests Functional#apply with a deletion mutation; we traverse all keys (in
	 * each direction) to make sure the count is correct and keys and values
	 * match up.
	 */
	public void testApplyDeletionMutation() throws Exception {
		final AtomicLong counter = new AtomicLong();
		final Exchange exchange = getExchange(db, true);

		Traversal<String, Integer, Integer> traversalAscending = Functional
				.map(exchange, getFullTraversal(Direction.ASC),
						getIdentityMapping(counter, true, true));

		while (traversalAscending.hasNext()) {
			Integer value = traversalAscending.next();
			Assert.assertEquals((int) counter.get() - 1, value.intValue());
		}

		Assert.assertEquals(counter.get(), 1000);

		final Template<String, Integer> template = new Template<String, Integer>();

		final Mapping<String, Integer, Mutation<String, Integer>> deletionMutation = new Mapping<String, Integer, Functional.Mutation<String, Integer>>() {
			@Override
			public Mutation<String, Integer> map(Pair<String, Integer> row) {
				if (row.getValue() % 2 == 1) {
					return new Mutation<String, Integer>(MutationType.DELETE,
							row);
				}
				return null;
			}
		};

		final Exchange source = getExchange(db, true);
		final Exchange target = getExchange(db, true);

		template.inTransaction(db, new TransactionCallback<Void>() {
			@Override
			public Void inTransaction(Transaction txn) {
				Functional.apply(source, template, deletionMutation, target)
						.traverseAll();

				return null;
			}
		});

		counter.set(0);

		Traversal<String, Integer, Integer> traversalDescending = Functional
				.map(exchange, getFullTraversal(Direction.DESC),
						getIdentityMapping(counter, true, false));

		while (traversalDescending.hasNext()) {
			Integer value = traversalDescending.next();
			Assert.assertEquals(1000 - (2 * (int) counter.get()),
					value.intValue());
		}

		Assert.assertEquals(counter.get(), 500);
	}
}
