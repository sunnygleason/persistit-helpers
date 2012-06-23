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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.g414.persistit.Functional.Direction;
import com.g414.persistit.Functional.Filter;
import com.g414.persistit.Functional.Mapping;
import com.g414.persistit.Functional.Pair;
import com.g414.persistit.Functional.TraversalSpec;
import com.g414.persistit.Template.TransactionCallback;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

public class FunctionalTestBase {
	protected static String vol = "dude";
	protected static String tree = "awesome";
	protected static Persistit db = null;

	@BeforeMethod
	public void init() throws Exception {
		db = getPersistit();
		initializePersistit(db);
	}

	@AfterMethod
	public void finish() throws Exception {
		db.close();
		db = null;
	}

	public Persistit getPersistit() {
		try {
			Persistit persistit = new Persistit();
			persistit.initialize();

			return persistit;
		} catch (PersistitException e) {
			throw new RuntimeException(e);
		}
	}

	protected static void initializePersistit(Persistit persistit)
			throws Exception {
		final Exchange exchange = persistit.getExchange(vol, tree, true);
		exchange.removeAll();

		final Template<String, Integer> template = new Template<String, Integer>();

		TransactionCallback<Void> insertionCallback = new TransactionCallback<Void>() {
			@Override
			public Void inTransaction(Transaction txn) {
				for (int i = 0; i < 1000; i++) {
					template.insert(exchange, getKey(i), i);
				}

				return null;
			}
		};

		template.<Void> inTransaction(db, insertionCallback);
		persistit.releaseExchange(exchange);
	}

	protected static Exchange getExchange(Persistit persistit, boolean doCreate)
			throws Exception {
		return db.getExchange(vol, tree, doCreate);
	}

	protected static String getKey(int value) {
		return String.format("Key:%04d", value);
	}

	protected static TraversalSpec<String, Integer> getFullTraversal(
			Direction direction) {
		return new TraversalSpec<String, Integer>(direction, null, null);
	}

	protected static TraversalSpec<String, Integer> getFilteredTraversal(
			Direction direction, Filter<String, Integer> filter) {
		return new TraversalSpec<String, Integer>(direction, null, filter);
	}

	protected Mapping<String, Integer, Integer> getIdentityMapping(
			final AtomicLong counter, final boolean doIncrement,
			final boolean verifyKeyValues) {
		return new Mapping<String, Integer, Integer>() {
			@Override
			public Integer map(Pair<String, Integer> row) {
				if (verifyKeyValues) {
					Assert.assertEquals(row.getKey(), getKey(row.getValue()));
				}

				if (doIncrement) {
					counter.getAndIncrement();
				}

				return row.getValue();
			}
		};
	}
}
