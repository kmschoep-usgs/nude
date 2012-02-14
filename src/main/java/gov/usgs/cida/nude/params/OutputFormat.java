package gov.usgs.cida.nude.params;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.webservices.framework.basic.FormatType;

public enum OutputFormat implements Column {
	FORMAT_TYPE(FormatType.class),
	SCHEMA_NAME;

	private Class<?> valueType;
	
	private OutputFormat() {
		this(String.class);
	}
	
	private <T> OutputFormat(Class<T> valueType) {
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

	@Override
	public boolean isDisplayable() {
		return true;
	}
	
}
