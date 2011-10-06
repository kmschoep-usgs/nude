package examples.ida;

import examples.ida.response.IdaMetadata;
import gov.usgs.cida.nude.connector.http.AbstractHttpConnector;
import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.nude.provider.http.HttpProvider;
import gov.usgs.cida.nude.resultset.http.HttpResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.spec.jsl.Spec;
import gov.usgs.cida.spec.jsl.mapping.ColumnMapping;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdaMetadataConnector extends AbstractHttpConnector {
	private static final Logger log = LoggerFactory.getLogger(IdaMetadataConnector.class);
	protected String url;
	
	public IdaMetadataConnector(HttpProvider httpProvider) {
		super(httpProvider);
		
		this.url = "http://ida.water.usgs.gov/ida/available_records.cfm";
	}
	
	public Spec getSpec() {
		return new Spec() {
			
			protected ColumnMapping[] columns = new ColumnMapping[] {
					new ColumnMapping(IdaMetadata.MINDATETIME.getName(), "mindatetime"),
					new ColumnMapping(IdaMetadata.MAXDATETIME.getName(), "maxdatetime")
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
		String result = baseUrl;
		
		List<String> params = new ArrayList<String>();
		for (ResultSet rs : inputs) {
			try {
				if (rs.isWrapperFor(Iterable.class)) {
					String genParams = generateGetParams((Iterable<TableRow>) rs.unwrap(Iterable.class));
					if (StringUtils.isNotBlank(genParams)) {
						params.add(genParams);
					}
				} else {
					log.error("Could not unwrap input");
				}
			} catch (SQLException e) {
				log.error("Could not unwrap input", e);
			}
		}
		
		String queryString = StringUtils.join(params, '&');
		
		if (StringUtils.isNotBlank(queryString)) {
			result += "?" + queryString;
		}
		
		log.debug("Built URI: {}", result);
		return result;
	}

	@Override
	public IParser getParser() {
		return new IdaParser();
	}

	public Integer getRowCount() {
		return new Integer(1);
	}

	@Override
	public boolean isReady() {
		// TODO Auto-generated method stub
		return false;
	}
}
