package gov.usgs.cida.nude.filter.transform;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

/**
 * 
 * @author dmsibley
 */
public class MuxByNotNullTransform extends MuxTransform {
	public MuxByNotNullTransform(Iterable<Column> columns) {
		super(columns);
	}

	@Override
	public String transform(TableRow row) {
		String result = null;
		
		for (Column col : columns) {
			if (null == result) {
				result = row.getValue(col);
			}
		}

		return result;
	}

}
