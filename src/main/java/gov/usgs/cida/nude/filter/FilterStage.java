package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

public class FilterStage {
	protected final ColumnGrouping inColumns;
	protected final Map<Column, ColumnAlias> transforms;
	protected final ColumnGrouping outColumns;
	
	public FilterStage(ColumnGrouping in, Map<Column, ColumnAlias> transform, ColumnGrouping out) {
		this.inColumns = in;
		this.transforms = Collections.unmodifiableMap(transform);
		this.outColumns = out;
	}
	
	public ColumnGrouping getInputColumns() {
		return this.inColumns;
	}
	
	public ColumnGrouping getOutputColumns() {
		return this.outColumns;
	}
	
	public String transform(int columnIndex, TableRow in) throws SQLException {
		String result = null;
		
		Column outCol = this.outColumns.get(columnIndex);
		result = this.transforms.get(outCol).transform(in);
		
		return result;
	}
	
}
