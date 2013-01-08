package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.resultset.inmemory.TableRow;

/**
 *
 * @author dmsibley
 */
public interface ColumnTransform {
	public String transform(TableRow row);
}
