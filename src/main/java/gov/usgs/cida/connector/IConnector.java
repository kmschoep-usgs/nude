package gov.usgs.cida.connector;

import gov.usgs.cida.connector.parser.IParser;
import gov.usgs.cida.spec.jsl.Spec;

import java.sql.ResultSet;

public interface IConnector {
	public void addInput(ResultSet in);
	public ResultSet getResultSet();
	public Integer getRowCount();
	public IParser<? extends Enum<?>> getParser();
	public Spec getSpec();
}
