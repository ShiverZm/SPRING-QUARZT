package org.dreams.fly.common;

public class Tuple<K, V, T> extends Pair<K, V> {

	private static final long serialVersionUID = -1597562070995719621L;

	private T attachment;

	public Tuple(){

	}

	public Tuple(K key, V value, T attachment){
		this.key = key;
		this.value = value;
		this.attachment = attachment;
	}

	public Tuple(TupleBuilder<K, V, T> tupleBuilder){
		this.key = tupleBuilder.key;
		this.value = tupleBuilder.value;
		this.attachment = tupleBuilder.attachment;
	}

	public T getAttachment() {
		return attachment;
	}

	public void setAttachment(T attachment) {
		this.attachment = attachment;
	}

	public static class TupleBuilder<K, V, T> extends PairBuilder<K, V> {

		private T attachment;

		public TupleBuilder(){

		}

		public TupleBuilder<K, V, T> attachment(T attachment){
			this.attachment = attachment;
			return this;
		}

		public Tuple<K, V, T> build(){
			return new Tuple<>(this);
		}

	}

	@Override
	public String toString() {
		return "Tuple [attachment=" + attachment + ", key=" + key + ", value=" + value + "]";
	}

}
