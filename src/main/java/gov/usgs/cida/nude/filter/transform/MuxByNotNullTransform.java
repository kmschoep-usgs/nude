/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.usgs.cida.nude.filter.transform;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author dmsibley
 */
public class MuxByNotNullTransform implements ColumnTransform {
	private static final Logger log = LoggerFactory.getLogger(MuxByNotNullTransform.class);
	
	private final List<Column> columns;

	public MuxByNotNullTransform(Iterable<Column> columns) {
		List<Column> cols = new ArrayList<Column>();
		
		if (null != columns) {
			for (Column col : columns) {
				cols.add(col);
			}
		}
		
		this.columns = Collections.unmodifiableList(cols);
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
