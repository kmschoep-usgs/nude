package gov.usgs.cida.nude.gel;

import gov.usgs.cida.nude.gel.transforms.GelTransform;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;

import java.util.Map;

public class GelBuilder {
	
	protected Map<Column, GelTransform> transforms;
	
	public GelBuilder(ColumnGrouping input) {
		//populate transforms with input
	}
	
	public void addGelTransform(Column outColumn, GelTransform transform) {
		this.transforms.put(outColumn, transform);
	}
	
	public Gel buildGel() {
		return new Gel(this.transforms);
	}
}
