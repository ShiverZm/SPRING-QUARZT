package org.dreams.fly.common;

import java.io.Serializable;

public class Pair<K, V> implements Serializable {

	private static final long serialVersionUID = 458139277078916334L;

	protected K key;

	protected V value;

	public Pair(){

	}

	public Pair(K key, V value){
		this.key = key;
		this.value = value;
	}

	private Pair(PairBuilder<K, V> builder){
		this.key = builder.key;
		this.value = builder.value;
	}

	public static class PairBuilder<K, V>{

		protected K key;
		protected V value;

		public PairBuilder(){

		}

		public PairBuilder<K, V> key(K key){
			this.key = key;
			return this;
		}

		public PairBuilder<K, V> value(V value){
			this.value = value;
			return this;
		}

		 public Pair<K, V> build() {
	            return new Pair<K, V>(this);
	     }

	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return " {key=" + key + ", value=" + value + "}";
	}

}
