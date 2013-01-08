package gov.usgs.cida.nude.connector.http;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.provider.http.HttpProvider;
import gov.usgs.cida.nude.resultset.http.HttpResultSet;
import gov.usgs.cida.nude.resultset.inmemory.StringTableResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHttpConnector implements HttpConnector {

	private static final Logger log = LoggerFactory.getLogger(AbstractHttpConnector.class);
	protected final HttpProvider httpProvider;
	protected List<ResultSet> inputs;

	public AbstractHttpConnector(HttpProvider httpProvider) {
		this.httpProvider = httpProvider;
		this.inputs = new ArrayList<ResultSet>();
	}

	@Override
	public void addInput(ResultSet in) {
		if (null != in) {
			this.inputs.add(in);
			this.fillRequiredInputs(in);
		} else {
			log.error("Input ResultSet was null!");
		}
	}

	protected abstract void fillRequiredInputs(ResultSet in);

	@Override
	public String getStatement() {
		return getURI(this);
	}
	
	protected abstract String getURI();

	//TODO change this to ResultSet instead of Iterable<TableRow>
	protected static String generateGetParams(Iterable<TableRow> params) {
		String result = null;

		List<String> kvps = new ArrayList<String>();
		for (TableRow row : params) {
			for (Entry<Column, String> entry : row.getEntries()) {
				String key = null;
				String value = null;

				key = StringUtils.lowerCase(entry.getKey().getName());
				value = entry.getValue();

				if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
					kvps.add(key + "=" + value);
				}
			}
		}

		result = StringUtils.join(kvps, '&');
		return result;
	}

	protected static List<NameValuePair> generatePostParams(Iterable<TableRow> params) {
		List<NameValuePair> result = new ArrayList<NameValuePair>();

		//TODO

		return result;
	}

	/**
	 * WARNING: BLOCKING
	 * @return 
	 */
	@Override
	public boolean isReady() {
		boolean result = false;

		String uri = getURI(this);
		try {
			int code = makeHeadCall(uri);
			if (200 == code) {
				result = true;
			} else {
				log.debug("Code " + code + " for service " + uri);
				//Close Expired Connections here, possibly fix
				//The non-reusable connection leak
			}

		} catch (Exception e) {
			log.error("Could not make HEAD request", e);
		}

		return result;
	}

	/**
	 * WARNING: BLOCKING
	 * @return 
	 */
	@Override
	public ResultSet getResultSet() {
		ResultSet result = null;

		String uri = getURI(this);
		if (null != uri) {
			log.info("Trying to get ResultSet: " + uri);
			try {
				if (isReady()) {
					HttpEntity methodEntity = makeGetCall();
					result = new HttpResultSet(methodEntity, this.getParser());
				} else {
					log.error("Source not ready: " + uri);
				}
			} catch (Exception e) {
				log.error("Could not make call", e);
			}
		}

		if (null == result) {
			result = new StringTableResultSet(this.getExpectedColumns());
		}

		return result;
	}

	/**
	 *
	 * @param con
	 * @return URI or null if invalid Input
	 */
	public static String getURI(AbstractHttpConnector con) {
		String result = null;

		if (con.isValidInput()) {
			result = con.getURI();
		}

		return result;
	}

	protected int makeHeadCall(String uri) throws ClientProtocolException, IOException {
		int result = -1;
		if (null != uri) {
			log.trace("Sending HEAD: " + uri);
			HttpResponse resp = null;
		
			HttpClient httpClient = httpProvider.getClient();
			HttpUriRequest req = new HttpHead(uri);
			HttpProvider.generateFirefoxHeaders(req, null);

			try {
				resp = httpClient.execute(req);
				result = resp.getStatusLine().getStatusCode();
			} finally {
				req.abort();
			}
		}
		
		
		return result;
	}

	public HttpEntity makeGetCall() throws ClientProtocolException, IOException {
		HttpEntity result = null;
		String uri = getURI();
		
		if (null != uri) {
			log.trace("Sending GET: " + uri);
			
			HttpClient httpClient = httpProvider.getClient();
			HttpUriRequest req = new HttpGet(uri);
			HttpProvider.generateFirefoxHeaders(req, null);

			result = makeCall(httpClient, req, null);
		}
		

		return result;
	}

	protected static HttpEntity makeCall(HttpClient httpClient, HttpUriRequest req, HttpContext localContext) throws ClientProtocolException, IOException {
		HttpEntity result = null;

		HttpResponse methodResponse = null;
		if (null == localContext) {
			methodResponse = httpClient.execute(req);
		} else {
			methodResponse = httpClient.execute(req, localContext);
		}

		HttpEntity methodEntity = methodResponse.getEntity();
		result = methodEntity;

		return result;
	}
}
