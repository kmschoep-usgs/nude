package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.filter.transform.ColumnAlias;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterStageBuilder {
	protected Map<Column, ColumnTransform> transforms;
	protected ColumnGrouping inColumns;
	protected Column primaryKey;
	protected List<Column> outColumns;
	
	public FilterStageBuilder(ColumnGrouping input) {
		this.inColumns = input;
		this.transforms = new HashMap<Column, ColumnTransform>();
		this.outColumns = new ArrayList<Column>();
		
		for (Column col : input) {
			this.addTransform(col, new ColumnAlias(col));
		}
		this.setPrimaryKey(this.inColumns.getPrimaryKey());
	}
	
	public FilterStageBuilder setPrimaryKey(Column outColumn) {
		if (this.transforms.containsKey(outColumn)) {
			this.primaryKey = outColumn;
		}
		return this;
	}
	
	public FilterStageBuilder addTransform(Column outColumn, ColumnTransform transform) {
		this.transforms.put(outColumn, transform);
		this.outColumns.add(outColumn);
		return this;
	}
	
	public FilterStage buildFilterStage() {
		ColumnGrouping outCols = new ColumnGrouping(this.primaryKey, this.outColumns);
		return new FilterStage(this.inColumns, this.transforms, outCols);
	}
}
