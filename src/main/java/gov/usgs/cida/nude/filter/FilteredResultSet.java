package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.column.CGResultSetMetaData;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.resultset.inmemory.PeekingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class FilteredResultSet extends PeekingResultSet {
	
	protected final FilterStage gel;
	protected final ResultSet in;
	
	public FilteredResultSet(ResultSet input, FilterStage transform) {
		try {
			this.closed = input.isClosed();
		} catch (SQLException e) {
			this.closed = true;
		}
		
		this.in = input;
		this.gel = transform;
				
		this.metadata = new CGResultSetMetaData(this.gel.outColumns);
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
	
	@Override
	protected void addNextRow() throws SQLException {
		boolean hasNext = in.next();
		if (hasNext) {
			this.nextRows.add(buildRow());
		}
	}

	@Override
	public void close() throws SQLException {
		try {
			in.close();
		} finally {
			this.closed = true;
		}
	}
	
	@Override
	public String getString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwIfBadLocation(loc);
		return this.gel.transform(columnIndex, this.currRow);
	}

	@Override
	public String getCursorName() throws SQLException {
		throwIfClosed(this);
		return in.getCursorName();
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.gel.outColumns.indexOf(columnLabel);
	}

}
