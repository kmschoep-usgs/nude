package gov.usgs.cida.nude.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ColumnGrouping implements Iterable<Column> {
	protected final Column primaryKeyColumn;
	protected final Map<String, Integer> colToIndex;
	protected final List<Column> columns;
	//When adding column groupings to eachother, the primary key column must match!
	
	public ColumnGrouping(Column primaryKeyColumn) {
		this(primaryKeyColumn, new ArrayList<Column>());
	}
	
	/**
	 * TODO NO DUPLICATE COLUMNS!
	 * @param primaryKeyColumn
	 * @param columns
	 */
	public ColumnGrouping(Column primaryKeyColumn, List<Column> columns) {
		this.primaryKeyColumn = primaryKeyColumn;
		if (null == this.primaryKeyColumn) {
			throw new RuntimeException("Y U NO PRIMARY KEY?");
		}
		
		List<Column> cols = columns;
		if (null == cols) {
			cols = new ArrayList<Column>();
		}
		
		if (1 > cols.size()) {
			cols.add(this.primaryKeyColumn);
		}
		
		this.columns = Collections.unmodifiableList(cols);
		
		
		Map<String, Integer> cti = new HashMap<String, Integer>();
		for (int i = 0; i < this.columns.size(); i++) { //TODO Bimap?
			cti.put(this.columns.get(i).getName(), new Integer(i));
		}
		
		this.colToIndex = Collections.unmodifiableMap(cti);
	}
	
	public Column getPrimaryKey() {
		return this.primaryKeyColumn;
	}
	
	public List<Column> getColumns() {
		return this.getColumns();
	}
	
	public ColumnGrouping join(ColumnGrouping columns) {
		ColumnGrouping result = null;
		
		if (null != columns && this.getPrimaryKey().equals(columns.getPrimaryKey())) {
			List<Column> cols = new ArrayList<Column>();
			cols.addAll(this.getColumns());
			cols.addAll(columns.getColumns());
			
			result = new ColumnGrouping(this.getPrimaryKey(), cols);
		}
		
		return result;
	}
	
	public Column get(int index) {
		return this.columns.get(index - 1);
	}
	
	public int size() {
		return this.columns.size();
	}
	
	public int indexOf(String columnName) {
		int result = -1;
		
		Integer colIndex = this.colToIndex.get(columnName);
		if (null != colIndex) {
			result = colIndex + 1;
		}
		
		return result;
	}

	@Override
	public Iterator<Column> iterator() {
		return this.columns.iterator();
	}
	
}
