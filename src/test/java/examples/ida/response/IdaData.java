package examples.ida.response;

import gov.usgs.cida.nude.column.Column;

public enum IdaData implements Column {
	site_no,
	date_time,
	tz_cd,
	dd,
	accuracy_cd,
	value,
	precision,
	remark;

	private Class<?> valueType;
	
	private IdaData() {
		this.valueType = String.class;
	}
	
	private <T> IdaData(Class<T> valueType) {
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
