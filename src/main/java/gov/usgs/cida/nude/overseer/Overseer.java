package gov.usgs.cida.nude.overseer;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

public abstract class Overseer {
	protected List<ResultSet> inputs = new ArrayList<ResultSet>();
	
	public void addInput(ResultSet in) {
		this.inputs.add(in);
	}
	
	public abstract void dispatch(Writer out) throws SQLException, XMLStreamException, IOException;
	
}
