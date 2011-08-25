package gov.usgs.cida.provider.http;

import gov.usgs.cida.provider.IProvider;

import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SchemeRegistryFactory;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpProvider implements IProvider {
	private static final Logger log = LoggerFactory.getLogger(HttpProvider.class);
	
	// Connection pool setup
	private final static int CONNECTION_TTL = 15 * 60 * 1000;       // 15 minutes, default is infinte
	private final static int CONNECTIONS_MAX_TOTAL = 256;
	private final static int CONNECTIONS_MAX_ROUTE = 32;
	// Connection timeouts
	private final static int CLIENT_SOCKET_TIMEOUT = 5 * 60 * 1000; // 5 minutes, default is infinite
	private final static int CLIENT_CONNECTION_TIMEOUT = 15 * 1000; // 15 seconds, default is infinte
	
	protected ThreadSafeClientConnManager clientConnectionManager = null;
	
	@Override
	public void init() {
		if (null != clientConnectionManager) {
			throw new IllegalStateException("Init ran on previously set up instance!");
		}
		
		// Initialize connection manager, this is thread-safe.  if we use this
		// with any HttpClient instance it becomes thread-safe.
		clientConnectionManager = new ThreadSafeClientConnManager(SchemeRegistryFactory.createDefault(), CONNECTION_TTL, TimeUnit.MILLISECONDS);
		clientConnectionManager.setMaxTotal(CONNECTIONS_MAX_TOTAL);
		clientConnectionManager.setDefaultMaxPerRoute(CONNECTIONS_MAX_ROUTE);
		log.info("Created HTTP client connection manager {}: maximum connections total = {}, maximum connections per route = {}",
				new Integer[] {clientConnectionManager.hashCode(), clientConnectionManager.getMaxTotal(), clientConnectionManager.getDefaultMaxPerRoute()});
		
	}

	/**
	 * Gets an HttpClient through the connection manager. Returns null when Provider has not been set up
	 * @return
	 */
	public HttpClient getClient() {
		HttpClient result = null;
		
		if (null != clientConnectionManager) {
			HttpParams httpParams = new BasicHttpParams();
			HttpConnectionParams.setSoTimeout(httpParams, CLIENT_SOCKET_TIMEOUT);
			HttpConnectionParams.setConnectionTimeout(httpParams, CLIENT_CONNECTION_TIMEOUT);

			result = new DefaultHttpClient(clientConnectionManager, httpParams);
		}
		
		return result;
	}
	
	@Override
	public void destroy() {
		int code = clientConnectionManager.hashCode();
		clientConnectionManager.shutdown();
		clientConnectionManager = null;
		log.info("Destroyed HTTP client connection manager {}", code);
	}

}
