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

import com.g414.persistit.Functional.Pair;
import com.g414.persistit.Functional.PairImpl;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

/**
 * Encapsulates mutations of Keys and Values using Persistit; eliminates much of
 * the boilerplate code of using Exchange.
 */
public class Template<K, V> {
	public interface TransactionCallback<T> {
		public T inTransaction(Transaction txn);
	}

	public interface ExchangeCallback<T> {
		public T run();
	}

	public <T> T inTransaction(Persistit database,
			TransactionCallback<T> callback) throws Exception {
		Transaction txn = null;
		try {
			txn = database.getTransaction();
			txn.begin();

			return callback.inTransaction(txn);
		} catch (Exception e) {
			if (txn != null) {
				txn.rollback();
				txn.end();
				txn = null;
			}

			throw e;
		} finally {
			if (txn != null) {
				txn.commit();
				txn.end();
				txn = null;
			}
		}
	}

	public <T> T withExchange(Persistit database, Exchange exchange,
			ExchangeCallback<T> callback) throws Exception {
		try {
			return callback.run();
		} finally {
			database.releaseExchange(exchange);
		}
	}

	public Pair<K, V> load(Exchange exchange, K key) {
		try {
			exchange.clear();
			exchange.getKey().append(key);
			exchange.fetch();

			if (!exchange.getValue().isDefined()) {
				return null;
			}

			return new PairImpl<K, V>((K) exchange.getKey().decode(),
					(V) exchange.getValue().get());
		} catch (PersistitException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean insert(Exchange exchange, K key, V value) {
		if (recordExists(exchange, key)) {
			return false;
		}

		return insertOrUpdate(exchange, key, value);
	}

	public boolean update(Exchange exchange, K key, V value) {
		if (!recordExists(exchange, key)) {
			return false;
		}

		return insertOrUpdate(exchange, key, value);
	}

	public boolean insertOrUpdate(Exchange exchange, K key, V value) {
		try {
			exchange.getKey().to(key);
			exchange.getValue().put(value);
			exchange.store();

			return true;
		} catch (PersistitException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean delete(Exchange exchange, K key) {
		if (!recordExists(exchange, key)) {
			return false;
		}

		try {
			exchange.getKey().to(key);

			return exchange.fetchAndRemove();
		} catch (PersistitException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean recordExists(Exchange exchange, K key) {
		try {
			exchange.clear();
			exchange.getKey().to(key);

			return exchange.isValueDefined();
		} catch (PersistitException e) {
			throw new RuntimeException(e);
		}
	}
}
