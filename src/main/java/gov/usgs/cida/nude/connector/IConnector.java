package gov.usgs.cida.nude.connector;

import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.nude.resultset.CGResultSet;

import java.sql.ResultSet;

public interface IConnector {
	public void addInput(ResultSet in);
	public CGResultSet getResultSet();
	public IParser getParser();
	public boolean isReady();
}
