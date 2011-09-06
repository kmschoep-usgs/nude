package gov.usgs.cida.nude.gel;

import gov.usgs.cida.nude.gel.transforms.GelTransform;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;

import java.util.HashMap;
import java.util.Map;

public class GelBuilder {
	//TODO add some way to have the primary key in here.
	protected Map<Column, GelTransform> transforms;
	
	public GelBuilder(ColumnGrouping input) {
		this.transforms = new HashMap<Column, GelTransform>();
		
		for (Column col : input) {
			this.transforms.put(col, new GelTransform(col));
		}
	}
	
	public void addGelTransform(Column outColumn, GelTransform transform) {
		this.transforms.put(outColumn, transform);
	}
	
//	public void filter(Column inputColumn) {
//		this.transforms.remove(inputColumn);
//	}
	
	public Gel buildGel() {
		return new Gel(this.transforms);
	}
}
