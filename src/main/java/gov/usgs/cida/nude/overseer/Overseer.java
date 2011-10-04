package gov.usgs.cida.nude.overseer;

import gov.usgs.cida.nude.resultset.CGResultSet;

import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;

import javax.xml.stream.XMLStreamException;

public abstract class Overseer {
	
	public abstract void addInput(CGResultSet in);
	
	public abstract void dispatch(Writer out) throws SQLException, XMLStreamException, IOException ;
	
}
