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

import java.util.Iterator;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;

/**
 * Utility methods to implement a Functional programming-oriented approach to
 * Persistit traversals and modifications (as close as possible with Java).
 */
public class Functional {
	/** Encapsulates a key of type K and value of type V */
	public interface Pair<K, V> {
		public K getKey();

		public V getValue();
	}

	/** Traversal direction (analogous to a cursor direction) */
	public enum Direction {
		ASC, DESC;
	}

	/** A Key/Value Traversal that produces a T at each iteration */
	public interface Traversal<K, V, T> extends Iterator<T> {
		public void traverseAll();
	}

	/** For Mutation mappings, the type of Mutation */
	public enum MutationType {
		NONE, INSERT_OR_UPDATE, DELETE;
	}

	/**
	 * A Mutation specifies a change of the specified type that should happen at
	 * the specified Key with the specified value.
	 */
	public static class Mutation<K, V> {
		private final MutationType type;
		private final Pair<K, V> instance;

		public Mutation(MutationType type, Pair<K, V> instance) {
			this.type = type;
			this.instance = instance;
		}

		public MutationType getType() {
			return type;
		}

		public Pair<K, V> getInstance() {
			return instance;
		}
	}

	/**
	 * Specifies a mapping of a Key/Value "row" to a user-specified type T.
	 */
	public interface Mapping<K, V, T> {
		public T map(Pair<K, V> row);
	}

	/**
	 * Specifies a single-step reduction that takes a row and initial value of
	 * type T and returns an accumulated value.
	 */
	public interface Reduction<K, V, T> {
		public T reduce(Pair<K, V> row, T initial);
	}

	/**
	 * A Filter is a user-specified filter that specifies whether a row should
	 * be matched by the traversal.
	 */
	public interface Filter<K, V> extends Mapping<K, V, Boolean> {
	}

	/**
	 * Specifies a Traversal, including the cursor Direction (ascending or
	 * descending), nullable KeyFilter as a primary bounding filter, and
	 * nullable secondary filter for row-specific filtering.
	 */
	public static class TraversalSpec<K, V> {
		private final Direction direction;
		private final KeyFilter primaryFilter;
		private final Filter<K, V> filter;

		public TraversalSpec() {
			this(Direction.ASC, null, null);
		}

		public TraversalSpec(Direction direction) {
			this(direction, null, null);
		}

		public TraversalSpec(Direction direction, KeyFilter primaryFilter,
				Filter<K, V> filter) {
			this.direction = direction;
			this.primaryFilter = primaryFilter;
			this.filter = filter;
		}

		public Direction getDirection() {
			return direction;
		}

		public KeyFilter getPrimaryFilter() {
			return primaryFilter;
		}

		public Filter<K, V> getFilter() {
			return filter;
		}
	}

	/**
	 * Immediately executes the given mapping for each row in the K/V space.
	 * (Other methods in this class typically return the traversal)
	 */
	public static <K, V, T> void foreach(final Exchange exchange,
			final Mapping<K, V, T> r) {
		map(exchange, null, r).traverseAll();
	}

	/**
	 * Immediately executes the given mapping for each row in the given
	 * TraversalSpec. (Other methods in this class typically return the
	 * traversal)
	 */
	public static <K, V, T> void foreach(final Exchange exchange,
			final TraversalSpec<K, V> traversalSpec, final Mapping<K, V, T> r) {
		map(exchange, traversalSpec, r).traverseAll();
	}

	/**
	 * Returns a mapping traversal over the entire K/V space. For each K/V pair,
	 * the mapping will be executed and the Traversal will return a value of
	 * type T.
	 */
	public static <K, V, T> Traversal<K, V, T> map(Exchange exchange,
			final Mapping<K, V, T> mapping) {
		return new TraversalImpl<K, V, T>(exchange, null, mapping);
	}

	/**
	 * Returns a mapping traversal over the given TraversalSpec. For each K/V
	 * pair in the specified set, the mapping will be executed and the Traversal
	 * will return a value of type T.
	 */
	public static <K, V, T> Traversal<K, V, T> map(Exchange exchange,
			final TraversalSpec<K, V> traversalSpec,
			final Mapping<K, V, T> mapping) {
		return new TraversalImpl<K, V, T>(exchange, traversalSpec, mapping);
	}

	/**
	 * Immediately executes a reduction traversal over the full K/V space. For
	 * each K/V pair in the space, the reduction will be executed over the key,
	 * value and so-far accumulated value, finally returning the accumulated
	 * value of type T.
	 */
	public static <K, V, T> T reduce(final Exchange exchange,
			final Reduction<K, V, T> reduction, final T initial) {
		return reduce(exchange, null, reduction, initial);
	}

	/**
	 * Immediately executes a reduction traversal over the given TraversalSpec.
	 * For each K/V pair in the specified set, the reduction will be executed
	 * over the key, value and so-far accumulated value, finally returning the
	 * accumulated value of type T.
	 */
	public static <K, V, T> T reduce(final Exchange exchange,
			final TraversalSpec<K, V> traversalSpec,
			final Reduction<K, V, T> reduction, final T initial) {
		MapReduction<K, V, T> mr = new MapReduction<K, V, T>(initial, reduction);
		Traversal<K, V, T> iter = map(exchange, traversalSpec, mr);

		while (iter.hasNext()) {
			iter.next();
		}

		return mr.getAccum();
	}

	/**
	 * Returns a mutation traversal over the entire K/V space. As the traversal
	 * iterates, for each K/V pair in the specified set, the mutation will be
	 * executed over the key and value. If the iterator is not called, the
	 * mutations will not be applied.
	 */
	public static <K, V> Traversal<K, V, Mutation<K, V>> apply(
			final Exchange source, final Template<K, V> dbt,
			final Mapping<K, V, Mutation<K, V>> mutation, final Exchange target) {
		return apply(source, dbt, null, mutation, target);
	}

	/**
	 * Returns a mutation traversal over the given TraversalSpec. As the
	 * traversal iterates, for each K/V pair in the specified set, the mutation
	 * will be executed over the key and value. If the iterator is not called,
	 * the mutations will not be applied.
	 */
	public static <K, V> Traversal<K, V, Mutation<K, V>> apply(
			final Exchange source, final Template<K, V> dbt,
			final TraversalSpec<K, V> traversalSpec,
			final Mapping<K, V, Mutation<K, V>> mutation, final Exchange target) {
		final Mapping<K, V, Mutation<K, V>> mapping = new Mapping<K, V, Mutation<K, V>>() {
			@Override
			public Mutation<K, V> map(Pair<K, V> row) {
				Mutation<K, V> m = mutation.map(row);
				if (m == null) {
					m = new Mutation<K, V>(MutationType.NONE, row);
				}

				switch (m.getType()) {
				case NONE:
					break;
				case INSERT_OR_UPDATE:
					dbt.insertOrUpdate(target, m.getInstance().getKey(), m
							.getInstance().getValue());
					break;
				case DELETE:
					dbt.delete(target, m.getInstance().getKey());
					break;
				default:
					throw new IllegalArgumentException();
				}

				return m;
			}
		};

		return new TraversalImpl<K, V, Mutation<K, V>>(source, traversalSpec,
				mapping);
	}

	/**
	 * Specifies a reduction using a given mapping and so-far accumulated value.
	 */
	private static class MapReduction<K, V, T> implements Mapping<K, V, T> {
		T accum;
		Reduction<K, V, T> r;

		public MapReduction(T initial, Reduction<K, V, T> r) {
			accum = initial;
			this.r = r;
		}

		public T map(Pair<K, V> row) {
			accum = r.reduce(row, accum);

			return accum;
		}

		public T getAccum() {
			return accum;
		}
	}

	/**
	 * General implementation of a functional traversal using a Persistit
	 * exchange.
	 */
	private static class TraversalImpl<K, V, T> implements Traversal<K, V, T> {
		private final KeyFilter primaryFilter;
		private final Filter<K, V> filter;
		private final Mapping<K, V, T> mapping;
		private final boolean isAscending;
		private Exchange exchange;
		private Pair<K, V> nextItem;

		public TraversalImpl(Exchange exchange,
				TraversalSpec<K, V> traversalSpec, Mapping<K, V, T> mapping) {
			if (traversalSpec == null) {
				traversalSpec = new TraversalSpec<K, V>();
			}

			this.exchange = exchange;
			this.primaryFilter = traversalSpec.getPrimaryFilter();
			this.filter = traversalSpec.getFilter();
			this.mapping = mapping;

			this.isAscending = traversalSpec.getDirection().equals(
					Direction.ASC);

			Key.EdgeValue edgeValue = this.isAscending ? Key.BEFORE : Key.AFTER;
			exchange.getKey().to(edgeValue);

			nextItem = advance();
		}

		private Pair<K, V> advance() {
			Pair<K, V> toReturn = null;

			for (;;) {
				boolean foundRow = false;

				try {
					if (this.primaryFilter == null) {
						if (this.isAscending) {
							if (!exchange.hasNext()) {
								return null;
							}

							foundRow = exchange.next();
						} else {
							if (!exchange.hasPrevious()) {
								return null;
							}

							foundRow = exchange.previous();
						}
					} else {
						try {
							Key.Direction direction = this.isAscending ? Key.Direction.GT
									: Key.Direction.LT;

							foundRow = exchange.traverse(direction,
									primaryFilter, Integer.MAX_VALUE);
						} catch (PersistitException e) {
							throw new RuntimeException(e);
						}

						foundRow = foundRow
								&& primaryFilter.selected(exchange.getKey());
					}

					if (!foundRow) {
						break;
					}

					toReturn = new PairImpl(exchange.getKey().decode(),
							exchange.getValue().get());

					if (filter == null || filter.map(toReturn)) {
						break;
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			return toReturn;
		}

		@Override
		public boolean hasNext() {
			return nextItem != null;
		}

		public T next() {
			if (nextItem == null) {
				throw new IllegalStateException("next() called on empty iter");
			}

			Pair<K, V> orig = nextItem;
			nextItem = advance();

			try {
				return mapping.map(orig);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void traverseAll() {
			while (hasNext()) {
				next();
			}
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/** Encapsulates a simple Key/Value pair */
	public static class PairImpl<K, V> implements Pair<K, V> {
		private final K key;
		private final V value;

		public PairImpl(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof Pair)) {
				return false;
			}

			Pair<?, ?> other = (Pair<?, ?>) obj;
			return other != null
					&& ((key == null && other.getKey() == null) || (key != null && key
							.equals(other.getKey())))
					&& ((value == null && other.getValue() == null) || (value != null && value
							.equals(other.getValue())));
		}

		@Override
		public int hashCode() {
			return ((key != null) ? key.hashCode() : Pair.class.hashCode())
					^ ((value != null) ? value.hashCode() : Pair.class
							.getName().hashCode());
		}

		@Override
		public String toString() {
			return "Pair{key=" + ((key != null) ? key.toString() : "null")
					+ ",value=" + ((value != null) ? value.toString() : "null")
					+ "}";
		}
	}
}
