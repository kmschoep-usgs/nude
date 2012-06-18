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
		String result = "";
		
		if (null != this.value) {
			result = this.value.toString();
		}
		
		return result;
	}

	/**
	 * TODO
	 * @param obj
	 * @return 
	 */
	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}
}
