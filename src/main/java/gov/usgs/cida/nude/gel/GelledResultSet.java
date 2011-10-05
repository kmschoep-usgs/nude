package gov.usgs.cida.nude.gel;

import gov.usgs.cida.nude.resultset.CGResultSetMetaData;
import gov.usgs.cida.nude.resultset.CursorLocation.Location;
import gov.usgs.cida.nude.resultset.StringValImplResultSet;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.values.TableRow;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class GelledResultSet extends StringValImplResultSet implements ResultSet {
	
	protected boolean isClosed;
	protected final Gel gel;
	protected final ResultSet in;
	protected final ResultSetMetaData metadata;
	
	protected TableRow currRow;
	
	protected final Queue<TableRow> nextRows;
	
	public GelledResultSet(ResultSet input, Gel transform) {
		try {
			this.isClosed = input.isClosed();
		} catch (SQLException e) {
			this.isClosed = true;
		}
		
		this.in = input;
		this.gel = transform;
				
		this.metadata = new CGResultSetMetaData(this.gel.outColumns);
		
		this.currRow = null;
		
		this.nextRows = new LinkedList<TableRow>();
	}
	
	protected TableRow buildRow() throws SQLException {
		TableRow result = null;
		
		Map<Column, String> row = new HashMap<Column, String>();
		
		for (Column col : gel.inColumns) {
			row.put(col, in.getString(col.getName()));
		}
		
		result = new TableRow(gel.inColumns, row);
		
		return result;
	}
	
	protected void addNextRow() throws SQLException {
		boolean hasNext = in.next();
		if (hasNext) {
			this.nextRows.add(buildRow());
		}
	}
	
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

	@Override
	public void close() throws SQLException {
		try {
			in.close();
		} finally {
			this.isClosed = true;
		}
	}
	
	@Override
	public String getString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.gel.transform(columnIndex, this.currRow);
	}

	@Override
	public String getCursorName() throws SQLException {
		throwIfClosed(this);
		return in.getCursorName();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throwIfClosed(this);
		return this.metadata;
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.gel.outColumns.indexOf(columnLabel);
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.isClosed;
	}

}
