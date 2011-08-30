package examples.ida;

import gov.usgs.cida.spec.table.Column;

public enum IdaTable implements Column {
	MAXDATETIME,
	MINDATETIME;
	
	public static final String TABLE_NAME = "IDA";
	public static final String SCHEMA_NAME = "EXAMPLES";
	
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
