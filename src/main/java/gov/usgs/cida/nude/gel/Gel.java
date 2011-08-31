package gov.usgs.cida.nude.gel;

import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;

import java.util.Collections;
import java.util.Map;

public class Gel {
	protected final ColumnGrouping input;
	
	protected final Map<Column, GelTransform> output;
	
	public Gel(ColumnGrouping in, Map<Column, GelTransform> out) {
		this.input = in;
		this.output = Collections.unmodifiableMap(out);
	}
	
	public GelledResultSet getResultSet() {
		GelledResultSet result = null;
		
		result = new GelledResultSet(this);
		
		return result;
	}
}
