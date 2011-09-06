package examples.ida.response;

import gov.usgs.cida.nude.table.Column;

public enum IdaData implements Column {
	site_no,
	date_time,
	tz_cd,
	dd,
	accuracy_cd,
	value,
	precision,
	remark;

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
