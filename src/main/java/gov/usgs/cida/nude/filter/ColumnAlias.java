package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

public class ColumnAlias implements ColumnTransform {
	
	protected Column inputCol;
	
	public ColumnAlias(Column in) {
		this.inputCol = in;
	}
	
	@Override
	public String transform(TableRow row) {
		String result = null;
		
		result = row.getValue(inputCol);
		
		return result;
	}
}
