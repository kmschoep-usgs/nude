package gov.usgs.cida.nude.resultset.inmemory;

import gov.usgs.cida.nude.column.CGResultSetMetaData;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.CursorLocation;
import gov.usgs.cida.nude.resultset.IndexImplResultSet;
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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Good lord.  Freakin unit test this!!!
 * @author dmsibley
 */
public class StringTableResultSet extends IndexImplResultSet implements Iterable<TableRow>, ResultSet {
	
	protected boolean isClosed = false;
	
	protected CursorLocation currLoc;
	protected ResultSetMetaData metadata;
	
	protected int fetchsize = 0;
	
	protected Collection<TableRow> rows;
	protected TableRow currRow;
	protected Iterator<TableRow> it;
	
	protected ColumnGrouping columns;
	
	/**
	 * Default construction.
	 * 
	 * Rows will be output in the same order they are put in.
	 */
	public StringTableResultSet(ColumnGrouping columns) {
		this(columns, new ArrayList<TableRow>());
	}
	
	/**
	 * Use this constructor when you need to specify a different
	 * ordering of your rows.
	 * @param rows
	 */
	public StringTableResultSet(ColumnGrouping columns, Collection<TableRow> rows) {
		this.currLoc = new CursorLocation();
		this.columns = columns;

		this.metadata = new CGResultSetMetaData(this.columns);
		
		this.rows = rows;
		this.currRow = null;
		this.it = null;
	}
	
	public void addRow(TableRow row) {
		this.rows.add(row);
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throwIfClosed(this);
		return this.metadata;
	}
	
	
	@Override
	public boolean next() throws SQLException {
		throwIfClosed(this);
		boolean result = false;
		if (this.rows != null && 0 < this.rows.size() && !this.isAfterLast()) {
			
			if (this.isFirst()) {
				this.currLoc.setLocation(Location.MIDDLE);
			} else if (this.isBeforeFirst()) {
				this.it = this.rows.iterator();
				if (null != this.it) {
					this.currLoc.setLocation(Location.FIRST);
				}
			}
			
			if (this.it != null && this.it.hasNext()) {
				this.currRow = this.it.next();
				if (!this.it.hasNext()) {
					this.currLoc.setLocation(Location.LAST);
				}
				result = true;
			} else {
				this.currLoc.setLocation(Location.AFTERLAST);
			}
			
		} else if ((this.rows == null || 0 >= this.rows.size()) && !this.isAfterLast()) {
			this.currLoc.setLocation(Location.AFTERLAST);
		}
		return result;
	}

	@Override
	public void close() throws SQLException {
		this.isClosed = true;
	}

	@Override
	public boolean wasNull() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return false;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		String result = null;
		if (null != currRow) {
			result = currRow.getValue(columns.get(columnIndex));
		} else {
			throw new SQLException("Cursor after last row");
		}
		return result;
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

	@Deprecated
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
	public String getCursorName() throws SQLException {
		throwIfClosed(this);
		return "" + this.hashCode();
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
		return null;
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		throwIfClosed(this);
		int result = -1;
		try {
			result = this.columns.indexOf(columnLabel);
			if (0 > result) {
				throw new SQLException("Invalid Column Label: " + columnLabel);
			}
		} catch (Exception e) {
			throw new SQLException(e);
		}
		
		return result;
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
		return (Location.BEFOREFIRST == this.currLoc.getLocation());
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		throwIfClosed(this);
		return (Location.AFTERLAST == this.currLoc.getLocation());
	}

	@Override
	public boolean isFirst() throws SQLException {
		throwIfClosed(this);
		return (Location.FIRST == this.currLoc.getLocation());
	}

	@Override
	public boolean isLast() throws SQLException {
		throwIfClosed(this);
		return (Location.LAST == this.currLoc.getLocation());
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throwIfClosed(this);
		this.fetchsize = rows;
	}

	@Override
	public int getFetchSize() throws SQLException {
		throwIfClosed(this);
		return this.fetchsize;
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
	public boolean isClosed() throws SQLException {
		return this.isClosed;
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

	@SuppressWarnings("unchecked")
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throwIfClosed(this);
		T result = null;
		if (Iterable.class.equals(iface)) {
			result = (T) this;
		} else {
			throw new SQLException("Instance is not an unwrappable into requested interface " + iface.getName());
		}
		return result;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throwIfClosed(this);
		boolean result = false;
		
		if (Iterable.class.equals(iface)) {
			result = true;
		}
		
		return result;
	}

	@Override
	public Iterator<TableRow> iterator() {
		return this.rows.iterator();
	}

}
