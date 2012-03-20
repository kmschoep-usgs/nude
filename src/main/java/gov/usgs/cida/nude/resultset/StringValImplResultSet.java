package gov.usgs.cida.nude.resultset;

import gov.usgs.cida.nude.resultset.CursorLocation.Location;

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
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;


public abstract class StringValImplResultSet extends IndexImplResultSet {

	public CursorLocation loc = new CursorLocation();
	
	@Override
	public boolean wasNull() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return 0;
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return 0;
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return 0;
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return 0;
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return 0;
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return 0;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
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
	public SQLWarning getWarnings() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		throwIfClosed(this);
	}
	
	@Override
	public Object getObject(int columnIndex) throws SQLException {
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
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		throwIfClosed(this);
		boolean result = false;
		
		result = (Location.BEFOREFIRST == this.loc.getLocation());
		
		return result;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		throwIfClosed(this);
		boolean result = false;
		
		result = (Location.AFTERLAST == this.loc.getLocation());
		
		return result;
	}

	@Override
	public boolean isFirst() throws SQLException {
		throwIfClosed(this);
		boolean result = false;
		
		result = (Location.FIRST == this.loc.getLocation());
		
		return result;
	}

	@Override
	public boolean isLast() throws SQLException {
		throwIfClosed(this);
		boolean result = false;
		
		result = (Location.LAST == this.loc.getLocation());
		
		return result;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
	}

	@Override
	public int getFetchSize() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return 0;
	}

	@Override
	public Statement getStatement() throws SQLException {
		throwIfClosed(this);
		return null;
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
		throwNotSupported();
		return null;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}
	
	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throwIfClosed(this);
		throw new SQLException("Instance is not an unwrappable into requested interface " + iface.getName());
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throwIfClosed(this);
		return false;
	}
	
	public static void throwIfBadLocation(CursorLocation loc) throws SQLException {
		Location currLoc = loc.getLocation();
		if (Location.BEFOREFIRST == currLoc) {
			throw new SQLException("Cursor positioned before first row");
		}
		if (Location.AFTERLAST == currLoc) {
			throw new SQLException("Cursor positioned after last row");
		}
	}
}
