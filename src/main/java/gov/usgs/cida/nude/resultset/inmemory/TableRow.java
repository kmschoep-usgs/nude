package gov.usgs.cida.nude.resultset.inmemory;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TableRow implements Comparable<TableRow>{
	protected final Map<Column, String> row;
	protected final ColumnGrouping columns;
	
	public TableRow(Column primaryKey, String value) {
		this.columns = new ColumnGrouping(primaryKey);
		this.row = new HashMap<Column, String>();
		this.row.put(primaryKey, value);
	}
	
	public TableRow(ColumnGrouping colGroup) {
		this(colGroup, null);
	}
	
	public TableRow(ColumnGrouping colGroup, Map<Column, String> row) {
		if (null == colGroup) {
			throw new RuntimeException("ColumnGroup cannot be null");
		}
		if (null == row) {
			row = new HashMap<Column, String>();
		}
		
		this.row = Collections.unmodifiableMap(row);
		this.columns = colGroup;
	}
	
	public String getValue(Column column) {
		return this.row.get(column);
	}
	
	public ColumnGrouping getColumns() {
		return this.columns;
	}
	
	public Set<Entry<Column, String>> getEntries() {
		return this.row.entrySet();
	}
	
	public Map<Column, String> getMap() {
		return this.row;
	}

	/**
	 * Compares the values of the primary keys. (Values are compared as Strings)
	 */
	@Override
	public int compareTo(TableRow o) {
		return this.getValue(this.getColumns().getPrimaryKey()).compareTo(o.getValue(this.getColumns().getPrimaryKey()));
	}
}
