package gov.usgs.cida.nude.overseer;

import java.io.Writer;
import java.sql.ResultSet;

public abstract class Overseer {
	
	public abstract void addInput(ResultSet in);
	
	public abstract void dispatch(Writer out);
	
}
