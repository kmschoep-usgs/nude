package gov.usgs.cida.resultset;

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
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;


public abstract class IndexImplResultSet extends ReadOnlyForwardResultSet {
	
	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getArray(this.findColumn(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getAsciiStream(this.findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getBigDecimal(this.findColumn(columnLabel));
	}

	@Deprecated
	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return this.getBigDecimal(this.findColumn(columnLabel), scale);
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getBinaryStream(this.findColumn(columnLabel));
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getBlob(this.findColumn(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getBoolean(this.findColumn(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getByte(this.findColumn(columnLabel));
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getBytes(this.findColumn(columnLabel));
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getCharacterStream(this.findColumn(columnLabel));
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getClob(this.findColumn(columnLabel));
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
	public double getDouble(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getDouble(this.findColumn(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getFloat(this.findColumn(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getInt(this.findColumn(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getLong(this.findColumn(columnLabel));
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getNCharacterStream(this.findColumn(columnLabel));
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getNClob(this.findColumn(columnLabel));
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getNString(this.findColumn(columnLabel));
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
	public Ref getRef(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getRef(this.findColumn(columnLabel));
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getRowId(this.findColumn(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getShort(this.findColumn(columnLabel));
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getSQLXML(this.findColumn(columnLabel));
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getString(this.findColumn(columnLabel));
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

	@Deprecated
	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return this.getUnicodeStream(this.findColumn(columnLabel));
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.getURL(this.findColumn(columnLabel));
	}
	
}
