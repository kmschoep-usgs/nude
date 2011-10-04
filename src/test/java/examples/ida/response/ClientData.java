package examples.ida.response;

import gov.usgs.cida.nude.table.Column;

public enum ClientData implements Column {
	timestamp,
	value;

	private Class<?> valueType;
	
	private ClientData() {
		this.valueType = String.class;
	}
	
	private <T> ClientData(Class<T> valueType) {
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
