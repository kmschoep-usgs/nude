package gov.usgs.cida.nude.connector;

import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.spec.jsl.Spec;

import java.sql.ResultSet;

public interface IConnector {
	public void addInput(ResultSet in);
	public ResultSet getResultSet();
	public IParser getParser();
}
