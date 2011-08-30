package examples.ida;

import gov.usgs.cida.connector.http.AbstractHttpConnector;
import gov.usgs.cida.connector.parser.IParser;
import gov.usgs.cida.provider.http.HttpProvider;
import gov.usgs.cida.resultset.http.HttpResultSet;
import gov.usgs.cida.spec.jsl.Spec;
import gov.usgs.cida.spec.jsl.mapping.ColumnMapping;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdaConnector extends AbstractHttpConnector {
	private static final Logger log = LoggerFactory
			.getLogger(IdaConnector.class);
	protected String url;
	protected List<ResultSet> inputs;
	
	public IdaConnector(HttpProvider httpProvider) {
		super(httpProvider);
		
		this.url = "http://ida.water.usgs.gov/ida/available_records.cfm";
		this.inputs = new ArrayList<ResultSet>();
	}
	
	public Spec getSpec() {
		return new Spec() {
			
			protected ColumnMapping[] columns = new ColumnMapping[] {
					new ColumnMapping("MINDATETIME", "mindatetime"),
					new ColumnMapping("MAXDATETIME", "maxdatetime")
			};
			
			@Override
			public ColumnMapping[] getColumns() {
				return this.columns;
			}
		};
	}
	
	@Override
	public ResultSet getResultSet() {
		ResultSet result = null;
		
		try {
			HttpEntity methodEntity = makeCall();
			result = new HttpResultSet(methodEntity, this.getParser());
		} catch (Exception e) {
			log.error("Could not make call", e);
		}
		
		return result;
	}
	
	public Integer getRowCount() {
		return new Integer(1);
	}
	
	@Override
	public void addInput(ResultSet in) {
		inputs.add(in);
	}
	
	public HttpEntity makeCall() throws ClientProtocolException, IOException {
		HttpEntity result = null;
		
		HttpClient httpClient = httpProvider.getClient();
		HttpUriRequest req = new HttpGet(getURI(this.url, this.inputs));
		generateFirefoxHeaders(req, null);
		
		result = makeCall(httpClient, req, null);
		
		return result;
	}
	
	public static String getURI(String baseUrl, List<ResultSet> inputs) {
		String result = null;
		
		StringBuffer sb = new StringBuffer();
		sb.append(baseUrl);
		
		//TODO
		sb.append("?sn=04085427");
		
		result = sb.toString();
		
		return result;
	}

	@Override
	public IParser<IdaTable> getParser() {
		return new IdaParser();
	}
}
