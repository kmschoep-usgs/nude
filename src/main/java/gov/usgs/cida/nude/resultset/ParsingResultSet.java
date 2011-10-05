package gov.usgs.cida.nude.resultset;

import gov.usgs.cida.nude.connector.parser.IParser;

import java.io.InputStream;
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

public abstract class ParsingResultSet extends IndexImplResultSet implements ResultSet {
	protected boolean isClosed = false;
	
	private ResultSetMetaData metadata;
	
	protected IParser parser;
	
	//TODO actually make this mean something
	protected int fetchSize = 0;
	
	@Override
	public int findColumn(String columnLabel) throws SQLException {
		throwIfClosed(this);
		int result = -1;
		
		try {
			result = this.parser.getAvailableColumns().indexOf(columnLabel);
		} catch (Exception e) {
			throw new SQLException(e);
		}
		
		return result;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throwIfClosed(this);
		if (null == this.metadata) {
			this.metadata = new CGResultSetMetaData(this.parser.getAvailableColumns());
		}
		return this.metadata;
	}
	
	@Override
	public void close() throws SQLException {
		this.isClosed = true;
	}
	
	@Override
	public boolean isClosed() throws SQLException {
		return this.isClosed;
	}
	
	@Override
	public boolean wasNull() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throwIfClosed(this);
		this.fetchSize = rows;
	}

	@Override
	public int getFetchSize() throws SQLException {
		throwIfClosed(this);
		return this.fetchSize;
	}

	@Override
	public void clearWarnings() throws SQLException {
		throwIfClosed(this);

	}
	
	@Override
	public SQLWarning getWarnings() throws SQLException {
		throwIfClosed(this);
		return null;
	}
	
	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Array.class, columnIndex).getValue();
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(BigDecimal.class, columnIndex).getValue();
	}

	@Deprecated
	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Blob.class, columnIndex).getValue();
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
	public byte getByte(int columnIndex) throws SQLException {
		throwIfClosed(this);
		Byte bt = this.parser.getValue(Byte.class, columnIndex).getValue();
		byte result = 0;
		if (null != bt) {
			result = bt.byteValue();
		}
		return result;
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Clob.class, columnIndex).getValue();
	}

	@Override
	public String getCursorName() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
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
		throwNotSupported();
		return null;
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		throwIfClosed(this);
		Double db = this.parser.getValue(Double.class, columnIndex).getValue();
		double result = 0.0;
		if (null != db) {
			result = db.doubleValue();
		}
		return result;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		throwIfClosed(this);
		Float fl = this.parser.getValue(Float.class, columnIndex).getValue();
		float result = 0;
		if (null != fl) {
			result = fl.floatValue();
		}
		return result;
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		throwIfClosed(this);
		Integer in = this.parser.getValue(Integer.class, columnIndex).getValue();
		int result = 0;
		if (null != in) {
			result = in.intValue();
		}
		return result;
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throwIfClosed(this);
		Long ln = this.parser.getValue(Long.class, columnIndex).getValue();
		long result = 0L;
		if (null != ln) {
			result = ln.longValue();
		}
		return result;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(NClob.class, columnIndex).getValue();
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(String.class, columnIndex).getValue();
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
		throwNotSupported();
		return null;
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Ref.class, columnIndex).getValue();
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throwIfClosed(this);
		Short sh = this.parser.getValue(Short.class, columnIndex).getValue();
		short result = 0;
		if (null != sh) {
			result = sh.shortValue();
		}
		return result;
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(SQLXML.class, columnIndex).getValue();
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(String.class, columnIndex).getValue();
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(Time.class, columnIndex).getValue();
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
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
		throwNotSupported();
		return null;
	}

	@Deprecated
	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.parser.getValue(URL.class, columnIndex).getValue();
	}

	@Override
	public Statement getStatement() throws SQLException {
		throwIfClosed(this);
		return null;
	}

}
