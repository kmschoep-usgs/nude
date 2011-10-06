package gov.usgs.cida.nude.gel;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.gel.transforms.GelTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

public class Gel {
	protected final ColumnGrouping inColumns;
	protected final Map<Column, GelTransform> transforms;
	protected final ColumnGrouping outColumns;
	
	public Gel(ColumnGrouping in, Map<Column, GelTransform> transform, ColumnGrouping out) {
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
