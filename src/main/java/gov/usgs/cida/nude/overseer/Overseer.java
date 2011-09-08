package gov.usgs.cida.nude.overseer;

import gov.usgs.cida.nude.resultset.ColumnGroupedResultSet;

import java.io.Writer;

public abstract class Overseer {
	
	public abstract void addInput(ColumnGroupedResultSet in);
	
	public abstract void dispatch(Writer out);
	
}
