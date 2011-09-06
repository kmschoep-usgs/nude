package examples.ida.request;

import java.util.Date;

import gov.usgs.cida.nude.table.Column;

public enum IdaConnectorParams implements Column {
	SITE_NUMBER,
	FROM_DATE(Date.class),
	TO_DATE(Date.class);
	
	private Class<?> valueType;
	
	private IdaConnectorParams() {
		this(String.class);
	}
	
	private <T> IdaConnectorParams(Class<T> valueType) {
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
	
	public Class<?> getValueType() {
		return this.valueType;
	}

}
