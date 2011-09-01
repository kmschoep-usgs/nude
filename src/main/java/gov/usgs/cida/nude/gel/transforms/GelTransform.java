package gov.usgs.cida.nude.gel.transforms;

import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.values.TableRow;

import java.util.HashSet;
import java.util.Set;

public class GelTransform {
	
	protected Set<Column> inputColumns;
	
	public GelTransform(Column in) {
		this.inputColumns = new HashSet();
		this.inputColumns.add(in);
	}
	
	public String transform(TableRow row) {
		Column inCol = inputColumns.iterator().next();
		row.getValue(inCol);
	}
}
