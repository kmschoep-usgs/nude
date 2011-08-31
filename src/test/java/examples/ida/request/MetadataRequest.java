package examples.ida.request;

import gov.usgs.cida.nude.table.Column;

public enum MetadataRequest implements Column {
	SN;

	public static final String TABLE_NAME = "REQUEST";
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

}
