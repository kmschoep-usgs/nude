package gov.usgs.cida.nude.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnGrouping {
	protected Column primaryKeyColumn;
	protected Map<String, Integer> colToIndex;
	protected List<Column> columns;
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
		
		this.columns = columns;
		if (null == this.columns) {
			this.columns = new ArrayList<Column>();
		}
		
		if (1 > this.columns.size()) {
			this.columns.add(this.primaryKeyColumn);
		}
		
		this.colToIndex = new HashMap<String, Integer>();
		for (int i = 0; i < this.columns.size(); i++) { //TODO Bimap?
			this.colToIndex.put(this.columns.get(i).getName(), new Integer(i));
		}
	}
	
	public Column getPrimaryKey() {
		return this.primaryKeyColumn;
	}
	
	public void addColumns(ColumnGrouping columns) {
		//TODO
	}
	
	public Column get(int index) {
		return this.columns.get(index);
	}
	
	public int size() {
		return this.columns.size();
	}
	
	public int indexOf(String columnName) {
		int result = -1;
		
		Integer colIndex = this.colToIndex.get(columnName);
		if (null != colIndex) {
			result = colIndex;
		}
		
		return result;
	}
}
