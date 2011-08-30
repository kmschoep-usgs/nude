package gov.usgs.cida.resultset.http;

import gov.usgs.cida.connector.parser.IParser;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpResultSet implements ResultSet {
	private static final Logger log = LoggerFactory
			.getLogger(HttpResultSet.class);
	protected boolean isClosed = false;
	protected final HttpEntity httpEntity;
	protected final BufferedReader serverResponseReader;
	protected final IParser<?> parser;

	public static void throwIfClosed(ResultSet rs) throws SQLException {
		if (rs.isClosed()) {
			throw new SQLException("Closed ResultSet");
		}
	}

	public static void throwNotSupported() throws SQLException {
		throw new SQLException("Operation not supported");
	}
	
	public HttpResultSet(HttpEntity httpEntity, IParser<?> responseParser) {
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
			//TODO!
//			Header contentHeader = this.httpEntity.getContentType();
//			if (null != contentHeader) {
//				String contentType = contentHeader.getValue();
//				int lastSemicolon = contentType.lastIndexOf(";");
//				int lastCharset = contentType.lastIndexOf("charset");
//			} 
			
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
	public int findColumn(String columnLabel) throws SQLException {
		int result = -1;
		
		try {
			result = Enum.valueOf(this.parser.getAvailableColumns(), columnLabel).ordinal();
		} catch (Exception e) {
			throw new SQLException(e);
		}
		
		return result;
	}
	
	@Override
	public void close() throws SQLException {
		this.isClosed = true;
		try {
			EntityUtils.consume(this.httpEntity);
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}
	
	@Override
	public boolean isClosed() throws SQLException {
		return this.isClosed;
	}

	@Override
	public boolean next() throws SQLException {
		throwIfClosed(this);
		return this.parser.next(this.serverResponseReader);
	}

	@Override
	public boolean wasNull() throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub

	}

	@Override
	public int getFetchSize() throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return 1;
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub

	}
	
	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("Instance is not an unwrappable object");
	}
	
	@Override
	public void beforeFirst() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void afterLast() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public boolean first() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public boolean last() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public int getRow() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return -1;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public boolean previous() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throwIfClosed(this);
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Array.class, columnIndex).getValue();
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getArray(this.findColumn(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getAsciiStream(this.findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(BigDecimal.class, columnIndex).getValue();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throwIfClosed(this);
//		return this.parser.getValue(BigDecimal.class, columnIndex).getValue();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getBigDecimal(this.findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		throwIfClosed(this);
		return this.getBigDecimal(this.findColumn(columnLabel), scale);
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getBinaryStream(this.findColumn(columnLabel));
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Blob.class, columnIndex).getValue();
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getBlob(this.findColumn(columnLabel));
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		throwIfClosed(this);
		boolean result = false;
		Boolean in = this.parser.getValue(Boolean.class, columnIndex).getValue();
		if (null != in) {
			result = in.booleanValue();
		}
		return result;
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getBoolean(this.findColumn(columnLabel));
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getByte(this.findColumn(columnLabel));
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getBytes(this.findColumn(columnLabel));
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getCharacterStream(this.findColumn(columnLabel));
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Clob.class, columnIndex).getValue();
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getClob(this.findColumn(columnLabel));
	}

	@Override
	public String getCursorName() throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Date.class, columnIndex).getValue();
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getDate(this.findColumn(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throwIfClosed(this);
		return this.getDate(this.findColumn(columnLabel), cal);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getDouble(this.findColumn(columnLabel));
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getFloat(this.findColumn(columnLabel));
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getInt(this.findColumn(columnLabel));
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getLong(this.findColumn(columnLabel));
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getNCharacterStream(this.findColumn(columnLabel));
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(NClob.class, columnIndex).getValue();
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getNClob(this.findColumn(columnLabel));
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(String.class, columnIndex).getValue();
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getNString(this.findColumn(columnLabel));
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Object.class, columnIndex).getValue();
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getObject(this.findColumn(columnLabel));
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throwIfClosed(this);
		return this.getObject(this.findColumn(columnLabel), map);
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Ref.class, columnIndex).getValue();
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getRef(this.findColumn(columnLabel));
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getRowId(this.findColumn(columnLabel));
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getShort(this.findColumn(columnLabel));
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(SQLXML.class, columnIndex).getValue();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getSQLXML(this.findColumn(columnLabel));
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(String.class, columnIndex).getValue();
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getString(this.findColumn(columnLabel));
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Time.class, columnIndex).getValue();
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getTime(this.findColumn(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throwIfClosed(this);
		return this.getTime(this.findColumn(columnLabel), cal);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Timestamp.class, columnIndex).getValue();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getTimestamp(this.findColumn(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		throwIfClosed(this);
		return this.getTimestamp(this.findColumn(columnLabel), cal);
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getUnicodeStream(this.findColumn(columnLabel));
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(URL.class, columnIndex).getValue();
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getURL(this.findColumn(columnLabel));
	}

	@Override
	public int getType() throws SQLException {
		throwIfClosed(this);
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public int getConcurrency() throws SQLException {
		throwIfClosed(this);
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void insertRow() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateRow() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void deleteRow() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void refreshRow() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public Statement getStatement() throws SQLException {
		throwIfClosed(this);
		return null;
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public int getHoldability() throws SQLException {
		throwIfClosed(this);
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

}
