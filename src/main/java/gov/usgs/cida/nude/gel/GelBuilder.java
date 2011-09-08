package gov.usgs.cida.nude.gel;

import gov.usgs.cida.nude.gel.transforms.GelTransform;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GelBuilder {
	protected Map<Column, GelTransform> transforms;
	protected ColumnGrouping inColumns;
	protected Column primaryKey;
	protected List<Column> outColumns;
	
	public GelBuilder(ColumnGrouping input) {
		this.inColumns = input;
		this.transforms = new HashMap<Column, GelTransform>();
		this.outColumns = new ArrayList<Column>();
		
		for (Column col : input) {
			this.addGelTransform(col, new GelTransform(col));
		}
		this.setGelPrimaryKey(this.inColumns.getPrimaryKey());
	}
	
	public void setGelPrimaryKey(Column outColumn) {
		if (this.transforms.containsKey(outColumn)) {
			this.primaryKey = outColumn;
		}
	}
	
	public void addGelTransform(Column outColumn, GelTransform transform) {
		this.transforms.put(outColumn, transform);
		this.outColumns.add(outColumn);
	}
	
//	public void filter(Column inputColumn) {
//		this.transforms.remove(inputColumn);
//	}
	
	public Gel buildGel() {
		ColumnGrouping outCols = new ColumnGrouping(this.primaryKey, this.outColumns);
		return new Gel(this.inColumns, this.transforms, outCols);
	}
}
