package examples.ida.request;

import gov.usgs.cida.nude.column.Column;

public enum DataRequest implements Column {
	fromdate,
	maxdatetime,
	mindatetime,
	rtype,
	site_no,
	submit1,
	todate;

	private Class<?> valueType;
	
	private DataRequest() {
		this.valueType = String.class;
	}
	
	private <T> DataRequest(Class<T> valueType) {
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
