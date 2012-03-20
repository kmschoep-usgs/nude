package gov.usgs.cida.nude.resultset.inmemory;

public class TypedValue<T> {
	protected final T value;
	
	public TypedValue(T val) {
		this.value = val;
	}
	
	public T getValue() {
		return this.value;
	}

	@Override
	public String toString() {
		return this.value.toString();
	}
}
