package gov.usgs.cida.connector.http;

import gov.usgs.cida.connector.IConnector;
import gov.usgs.cida.provider.http.HttpProvider;

import org.apache.http.client.methods.HttpUriRequest;

public abstract class AbstractHttpConnector implements IConnector {
	protected final HttpProvider httpProvider;
	
	public AbstractHttpConnector(HttpProvider httpProvider) {
		this.httpProvider = httpProvider;
	}
	
	protected static void generateFirefoxHeaders(HttpUriRequest req, String referer) {
		req.addHeader("User-Agent", "Mozilla/5.0 (X11; Linux x86_64; rv:5.0) Gecko/20100101 Firefox/5.0");
		req.addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		req.addHeader("Accept-Language", "en-us,en;q=0.5");
		req.addHeader("Accept-Encoding", "gzip, deflate");
		req.addHeader("Accept-Charset", "ISO-8859-1,utf-8;q=0.7,*;q=0.7");
		req.addHeader("Connection", "keep-alive");
		if (null != referer) {
			req.addHeader("Referer", referer);
		}
		req.addHeader("Pragma", "no-cache");
		req.addHeader("Cache-Control", "no-cache");
	}
	
//	protected static List<String> makeCall(HttpClient httpClient, HttpUriRequest req, HttpContext localContext) throws ClientProtocolException, IOException {
//		List<String> result = new ArrayList<String>();
//		
//		HttpResponse methodResponse = null;
//		if (null == localContext) {
//			methodResponse = httpClient.execute(req);
//		} else {
//			methodResponse = httpClient.execute(req, localContext);
//		}
//		
//		HttpEntity methodEntity = methodResponse.getEntity();
//		
//		if (methodEntity != null) {
//			
//			InputStream is = null;
//			try {
//				is = methodEntity.getContent();
//				
//				List<String> response = IOUtils.readLines(is);
//				
//				if (null != response) {
//					result = response;
//				}
//				
//			} finally {
//				// This is important to guarantee connection release back into
//                // connection pool for future reuse!
//                EntityUtils.consume(methodEntity);
//				
//				IOUtils.closeQuietly(is);
//			}
//			
//		}
//		
//		return result;
//	}
}
