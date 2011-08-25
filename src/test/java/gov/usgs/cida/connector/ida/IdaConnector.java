package gov.usgs.cida.connector.ida;

import java.sql.ResultSet;

import gov.usgs.cida.connector.http.AbstractHttpConnector;
import gov.usgs.cida.provider.http.HttpProvider;

public class IdaConnector extends AbstractHttpConnector {

	String url = "http://ida.water.usgs.gov/ida/available_records.cfm";
	
	public IdaConnector(HttpProvider httpProvider) {
		super(httpProvider);
	}
	
	@Override
	public ResultSet getResultSet() {
		return null;
	}

	@Override
	public void addInput(ResultSet in) {
		// TODO Auto-generated method stub
		
	}
}
