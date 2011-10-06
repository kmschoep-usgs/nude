package gov.usgs.cida.nude.resultset;

import gov.usgs.cida.nude.column.CGResultSetMetaData;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.CursorLocation.Location;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public abstract class PeekingResultSet extends StringValImplResultSet {
	
	protected boolean closed;
	
	protected ResultSetMetaData metadata;
	
	protected ColumnGrouping columns;
	
	protected TableRow currRow = null;
	protected final Queue<TableRow> nextRows = new LinkedList<TableRow>();
	
	@Override
	public boolean next() throws SQLException {
		throwIfClosed(this);
		boolean result = false;
		
		if (!this.isAfterLast()) {
			if (null == this.currRow) {
				addNextRow(); //Add an extra one if we're just starting off.
			}
			addNextRow();
			
			this.currRow = this.nextRows.poll();
			
			if (null == this.nextRows.peek()) {
				this.loc.setLocation(Location.LAST);
			}
			
			if (null == this.currRow) {
				result = false;
				this.loc.setLocation(Location.AFTERLAST);
			} else {
				result = true;
				if (this.isFirst()) {
					this.loc.setLocation(Location.MIDDLE);
				} else if (this.isBeforeFirst()) {
					this.loc.setLocation(Location.FIRST);
				}
			}
		}
		
		return result;
	}
	
	protected abstract void addNextRow() throws SQLException;
	
	@Override
	public void close() throws SQLException {
		this.closed = true;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwIfBadLocation(loc);
		return this.currRow.getValue(this.columns.get(columnIndex));
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throwIfClosed(this);
		if (null == this.metadata && null != this.columns) {
			this.metadata = new CGResultSetMetaData(this.columns);
		}
		return this.metadata;
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.columns.indexOf(columnLabel);
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.closed;
	}

}
