package examples.ida;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.connector.http.AbstractHttpConnector;
import gov.usgs.cida.nude.connector.parser.IParser;
import gov.usgs.cida.nude.provider.http.HttpProvider;
import gov.usgs.cida.nude.resultset.http.HttpResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdaMetadataConnector extends AbstractHttpConnector {
	private static final Logger log = LoggerFactory.getLogger(IdaMetadataConnector.class);
	protected String url;
	
	public IdaMetadataConnector(HttpProvider httpProvider) {
		super(httpProvider);
		
		this.url = "http://ida.water.usgs.gov/ida/available_records.cfm";
	}

	@Override
	public ColumnGrouping getExpectedColumns() {
		throw new UnsupportedOperationException("Not supported yet.");
	}
	
	@Override
	public ResultSet getResultSet() {
		ResultSet result = null;
		
		try {
			HttpEntity methodEntity = makeGetCall();
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
	
	@Override
	public String getURI() {
		return getURI(this.url, this.inputs);
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
		
		log.debug("Built URI: " + result);
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
	public boolean isValidInput() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected void fillRequiredInputs(ResultSet in) {
		//TODO
	}
}
