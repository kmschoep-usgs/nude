package examples.ida;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.connector.http.AbstractHttpConnector;
import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.nude.provider.http.HttpProvider;

import java.sql.ResultSet;

public class IdaDataConnector extends AbstractHttpConnector {

	protected String url;
	
	public IdaDataConnector(HttpProvider httpProvider) {
		super(httpProvider);
		
		this.url = "http://ida.water.usgs.gov/ida/available_records_process.cfm";
	}

	@Override
	public void addInput(ResultSet in) {
		// TODO Auto-generated method stub

	}

	@Override
	public ColumnGrouping getExpectedColumns() {
		throw new UnsupportedOperationException("Not supported yet.");
	}
	
	@Override
	public ResultSet getResultSet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IParser getParser() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isReady() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isValidInput() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected String getURI() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}
