package gov.usgs.cida.nude.resultset;

import gov.usgs.cida.nude.table.ColumnGrouping;

import java.sql.ResultSet;

public interface CGResultSet extends ResultSet {
	
	public ColumnGrouping getColumnGrouping();
	
}
