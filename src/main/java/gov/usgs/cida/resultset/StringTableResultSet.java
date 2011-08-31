package gov.usgs.cida.resultset;

import gov.usgs.cida.resultset.CursorLocation.Location;
import gov.usgs.cida.table.Column;
import gov.usgs.cida.values.TableRow;

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

public class StringTableResultSet<K extends Enum<K> & Column> extends IndexImplResultSet implements Iterable<TableRow<K>> {
	
	protected boolean isClosed = false;
	
	protected CursorLocation currLoc;
	
	protected int fetchsize = 0;
	
	protected Collection<TableRow<K>> rows;
	protected TableRow<K> currRow;
	protected Iterator<TableRow<K>> it;
	
	protected Class<K> tableType;
	protected K[] columns;
	
	/**
	 * Default construction.
	 * 
	 * Rows will be output in the same order they are put in.
	 */
	public StringTableResultSet(Class<K> table) {
		this(table, new ArrayList<TableRow<K>>());
	}
	
	/**
	 * Use this constructor when you need to specify a different
	 * ordering of your rows.
	 * @param rows
	 */
	public StringTableResultSet(Class<K> table, Collection<TableRow<K>> rows) {
		this.currLoc = new CursorLocation();
		this.tableType = table;
		this.columns = table.getEnumConstants();
		
		this.rows = rows;
		this.currRow = null;
		this.it = null;
	}
	
	public void addRow(TableRow<K> row) {
		this.rows.add(row);
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throwIfClosed(this);
		return new StringTableResultSetMetaData();
	}
	
	protected class StringTableResultSetMetaData implements ResultSetMetaData {
		
		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			throw new SQLException("Instance is not unwrappable to interface: " + iface.getName());
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return false;
		}

		@Override
		public int getColumnCount() throws SQLException {
			return columns.length;
		}

		@Override
		public boolean isAutoIncrement(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return false;
		}

		@Override
		public boolean isCaseSensitive(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return true;
		}

		@Override
		public boolean isSearchable(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return false;
		}

		@Override
		public boolean isCurrency(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return false;
		}

		@Override
		public int isNullable(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return ResultSetMetaData.columnNullableUnknown;
		}

		@Override
		public boolean isSigned(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return false;
		}

		@Override
		public int getColumnDisplaySize(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return 20;
		}

		@Override
		public String getColumnLabel(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return columns[column].getName();
		}

		@Override
		public String getColumnName(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return columns[column].getName();
		}

		@Override
		public String getSchemaName(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return columns[column].getSchemaName();
		}

		@Override
		public int getPrecision(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return 0;
		}

		@Override
		public int getScale(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return 0;
		}

		@Override
		public String getTableName(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return columns[column].getTableName();
		}

		@Override
		public String getCatalogName(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return "";
		}

		@Override
		public int getColumnType(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return java.sql.Types.VARCHAR;
		}

		@Override
		public String getColumnTypeName(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return "VARCHAR";
		}

		@Override
		public boolean isReadOnly(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return true;
		}

		@Override
		public boolean isWritable(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return false;
		}

		@Override
		public boolean isDefinitelyWritable(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return false;
		}

		@Override
		public String getColumnClassName(int column) throws SQLException {
			if (column >= this.getColumnCount()) {
				throw new SQLException("Invalid column index");
			}
			return String.class.getCanonicalName();
		}
		
	}
	
	@Override
	public boolean next() throws SQLException {
		throwIfClosed(this);
		boolean result = false;
		if (this.rows != null && this.isAfterLast()) {
			
			if (this.isFirst()) {
				this.currLoc.setLocation(Location.MIDDLE);
			} else if (this.isBeforeFirst()) {
				this.it = this.rows.iterator();
				if (null != this.it) {
					this.currLoc.setLocation(Location.FIRST);
				}
			}
			
			if (this.it.hasNext()) {
				this.currRow = this.it.next();
				if (this.it.hasNext()) {
					this.currLoc.setLocation(Location.LAST);
				}
				result = true;
			} else {
				this.currLoc.setLocation(Location.AFTERLAST);
			}
			
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
			result = currRow.getValue(columns[columnIndex]);
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
			result = Enum.valueOf(this.tableType, columnLabel).ordinal();
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
		throwNotSupported();
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
	public Iterator<TableRow<K>> iterator() {
		return this.rows.iterator();
	}

}
