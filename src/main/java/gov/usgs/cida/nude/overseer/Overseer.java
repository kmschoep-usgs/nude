package gov.usgs.cida.nude.overseer;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.xml.stream.XMLStreamException;

public abstract class Overseer {
	
	public abstract void addInput(ResultSet in);
	
	public abstract void dispatch(Writer out) throws SQLException, XMLStreamException, IOException ;
	
}
