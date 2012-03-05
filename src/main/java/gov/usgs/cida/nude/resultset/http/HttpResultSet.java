package gov.usgs.cida.nude.resultset.http;

import gov.usgs.cida.nude.connector.parser.IParser;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;
import org.apache.http.Header;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpResultSet extends ParsingResultSet {
	private static final Logger log = LoggerFactory.getLogger(HttpResultSet.class);
	
	protected final HttpEntity httpEntity;
	protected final Reader serverResponseReader;
	
	protected boolean isBeforeFirst = true;
	protected boolean isFirst = false;
	
	protected boolean isAfterLast = false;

	public static void throwIfClosed(ResultSet rs) throws SQLException {
		if (rs.isClosed()) {
			throw new SQLException("Closed ResultSet");
		}
	}

	public static void throwNotSupported() throws SQLException {
		throw new SQLException("Operation not supported");
	}
	
	public HttpResultSet(HttpEntity httpEntity, IParser responseParser) {
		this.httpEntity = httpEntity;
		
		InputStream in = null;
		try {
			in = this.httpEntity.getContent();
		} catch (Exception e) {
			log.error("Error getting response", e);
			if (null == in) {
				in = new ByteArrayInputStream(new byte[0]);
			}
		}
		
		InputStreamReader reader = null;
		try {
			String charset = null;

			Header contentHeader = this.httpEntity.getContentEncoding();
			if (null != contentHeader) {
				String encoding = contentHeader.getValue();
				log.trace("encoding:" + encoding);
				if ("gzip".equals(encoding)) {
					in = new GZIPInputStream(in);
				}
			}
			
			charset = "UTF-8";
			
			if (null != charset) {
				reader = new InputStreamReader(in, charset);
//			} else {
//				reader = new InputStreamReader(in);
			}
		} catch (Exception e) {
			log.error("Error decoding response", e);
		}
		
		this.serverResponseReader = new BufferedReader(reader);
		this.parser = responseParser;
	}
	
	@Override
	public void close() throws SQLException {
		super.close();
		try {
			EntityUtils.consume(this.httpEntity);
		} catch (IOException e) {
			log.error("Error closing HttpResultSet");
//			throw new SQLException(e);
		}
	}
	
	@Override
	public boolean isAfterLast() throws SQLException {
		throwIfClosed(this);
		return this.isAfterLast;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		throwIfClosed(this);
		return this.isBeforeFirst;
	}

	@Override
	public boolean isFirst() throws SQLException {
		throwIfClosed(this);
		return this.isFirst;
	}

	@Override
	public boolean isLast() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}
	
	@Override
	public boolean next() throws SQLException {
		throwIfClosed(this);
		boolean result = this.parser.next(this.serverResponseReader);
		
		if (result) {
			if (this.isBeforeFirst) {
				this.isBeforeFirst = false;
				this.isFirst = true;
				this.isAfterLast = false;
			} else if (this.isFirst) {
				this.isBeforeFirst = false;
				this.isFirst = false;
				this.isAfterLast = false;
			}
		} else {
			this.isBeforeFirst = false;
			this.isFirst = false;
			this.isAfterLast = true;
		}
		
		return result;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("Instance is not an unwrappable object");
	}

}
