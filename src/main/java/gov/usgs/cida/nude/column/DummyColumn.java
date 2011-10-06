package gov.usgs.cida.nude.column;

public enum DummyColumn implements Column {
	JOIN;

	private Class<?> valueType;
	
	private DummyColumn() {
		this.valueType = String.class;
	}
	
	private <T> DummyColumn(Class<T> valueType) {
		this.valueType = valueType;
	}
	
	@Override
	public String getName() {
		return this.toString();
	}

	@Override
	public String getQualifiedName() {
		return this.getName();
	}

	@Override
	public String getFullName() {
		return this.getName();
	}

	@Override
	public String getTableName() {
		return "";
	}

	@Override
	public String getSchemaName() {
		return "";
	}
	
	@Override
	public Class<?> getValueType() {
		return this.valueType;
	}

}
