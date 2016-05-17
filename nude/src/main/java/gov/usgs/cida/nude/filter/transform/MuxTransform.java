/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.usgs.cida.nude.filter.transform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;

/**
 *
 * @author dmsibley
 */
public abstract class MuxTransform implements ColumnTransform {
	protected final List<Column> columns;

	public MuxTransform(Iterable<Column> columns) {
		List<Column> cols = new ArrayList<Column>();

		if (null != columns) {
			for (Column col : columns) {
				cols.add(col);
			}
		}

		this.columns = Collections.unmodifiableList(cols);
	}

}
