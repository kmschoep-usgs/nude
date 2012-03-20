package gov.usgs.cida.nude.resultset;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

public abstract class ReadOnlyForwardResultSet implements ResultSet {

	public static void throwIfClosed(ResultSet rs) throws SQLException {
		if (null == rs || rs.isClosed()) {
			throw new SQLException("Closed ResultSet");
		}
	}

	public static void throwNotSupported() throws SQLException {
		throw new SQLException("Operation not supported");
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
	public int getHoldability() throws SQLException {
		throwIfClosed(this);
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}
	
	@Override
	public void refreshRow() throws SQLException {
		throwIfClosed(this);
		throwNotSupported();
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
