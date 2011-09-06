package examples.ida.request;

import gov.usgs.cida.nude.table.Column;

public enum DataRequest implements Column {
	fromdate,
	maxdatetime,
	mindatetime,
	rtype,
	site_no,
	submit1,
	todate;

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

}
