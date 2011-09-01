package gov.usgs.cida.nude.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ColumnGrouping {
	protected Column primaryKeyColumn;
	protected List<Column> columns;
	//When adding column groupings to eachother, the primary key column must match!
	
	public ColumnGrouping(Column primaryKeyColumn, List<Column> columns) {
		this.columns = columns;
		if (null == this.columns) {
			this.columns = new ArrayList<Column>();
		}
		this.primaryKeyColumn = primaryKeyColumn;
		if (null == this.primaryKeyColumn) {
			throw new RuntimeException("Y U NO PRIMARY KEY?");
		}
	}
	
	public Column getPrimaryKey() {
		return this.primaryKeyColumn;
	}
	
	public void addColumns(ColumnGrouping columns) {
		//TODO
	}
	
	public List<Column> getColumns() {
		return Collections.unmodifiableList(this.columns);
	}
}
