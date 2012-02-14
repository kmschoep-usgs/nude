package examples.ida.response;

import gov.usgs.cida.nude.column.Column;

public enum IdaMetadata implements Column {
	MINDATETIME,
	MAXDATETIME;
	
	private Class<?> valueType;
	
	private IdaMetadata() {
		this.valueType = String.class;
	}
	
	private <T> IdaMetadata(Class<T> valueType) {
		this.valueType = valueType;
	}
	
	public static final String TABLE_NAME = "RESPONSE";
	public static final String SCHEMA_NAME = "IDA_METADATA";
	
	@Override
	public String getName() {
		return toString();
	}

	@Override
	public String getQualifiedName() {
		return TABLE_NAME + "." + getName();
	}

	@Override
	public String getFullName() {
		return SCHEMA_NAME + "." + getQualifiedName();
	}

	@Override
	public String getTableName() {
		return TABLE_NAME;
	}

	@Override
	public String getSchemaName() {
		return SCHEMA_NAME;
	}
	
	@Override
	public Class<?> getValueType() {
		return this.valueType;
	}

	@Override
	public boolean isDisplayable() {
		return true;
	}

}
