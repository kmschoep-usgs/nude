package gov.usgs.cida.nude.column;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class ColumnGroupingBuilder {
	private static final Logger log = LoggerFactory.getLogger(ColumnGroupingBuilder.class);

	private Set<Column> checkCol;
	private List<Column> cols;
	private Column primaryKey;
	
	public ColumnGroupingBuilder() {
		this.cols = new ArrayList<Column>();
		this.checkCol = new HashSet<Column>();
	}

	public ColumnGroupingBuilder setPrimaryKey(Column primaryKey) {
		if (this.checkCol.add(primaryKey)) {
			this.cols.add(primaryKey);
		}
		this.primaryKey = primaryKey;
		return this;
	}
	
	public ColumnGroupingBuilder addColumn(Column column) {
		if (this.checkCol.add(column)) {
			this.cols.add(column);
		}
		return this;
	}
	
	public ColumnGrouping toColumnGrouping() {
		ColumnGrouping result = null;
		if (0 < this.cols.size()) {
			
			if (null != this.primaryKey) {
				result = new ColumnGrouping(this.primaryKey, this.cols);
			} else {
				result = new ColumnGrouping(cols);
			}
			
		}
		return result;
	}
}
