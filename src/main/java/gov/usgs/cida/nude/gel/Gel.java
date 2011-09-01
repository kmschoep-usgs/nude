package gov.usgs.cida.nude.gel;

import gov.usgs.cida.nude.gel.transforms.GelTransform;
import gov.usgs.cida.nude.table.Column;

import java.util.Collections;
import java.util.Map;

public class Gel {
	
	protected final Map<Column, GelTransform> transforms;
	
	public Gel(Map<Column, GelTransform> out) {
		this.transforms = Collections.unmodifiableMap(out);
	}
	
	public GelledResultSet getResultSet() {
		GelledResultSet result = null;
		
		result = new GelledResultSet(this);
		
		return result;
	}
}
