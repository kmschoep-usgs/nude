package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

import java.util.HashSet;
import java.util.Set;

public class ColumnAlias {
	
	protected Set<Column> inputColumns;
	
	public ColumnAlias(Column in) {
		this.inputColumns = new HashSet<Column>();
		this.inputColumns.add(in);
	}
	
	public String transform(TableRow row) {
		String result = null;
		
		Column inCol = inputColumns.iterator().next();
		result = row.getValue(inCol);
		
		return result;
	}
}
