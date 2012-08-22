package gov.usgs.cida.nude.filter;

import gov.usgs.cida.nude.column.CGResultSetMetaData;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.out.Closers;
import gov.usgs.cida.nude.resultset.inmemory.PeekingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilteredResultSet extends PeekingResultSet {
	private static final Logger log = LoggerFactory.getLogger(FilteredResultSet.class);
	
	protected final FilterStage filterStage;
	protected final ResultSet in;
	
	public FilteredResultSet(ResultSet input, FilterStage transform) {
		try {
			this.closed = input.isClosed();
		} catch (AbstractMethodError t) {
			log.trace("we're filtering a 1.4 version of ResultSet");
			this.closed = false;
		} catch (SQLException e) {
			this.closed = true;
		}
		
		this.in = input;
		this.filterStage = transform;
				
		this.metadata = new CGResultSetMetaData(this.filterStage.outColumns);
	}
	
	protected TableRow buildRow() throws SQLException {
		TableRow result = null;
		
		Map<Column, String> row = new HashMap<Column, String>();
		
		for (Column col : this.filterStage.inColumns) {
			row.put(col, this.in.getString(col.getName()));
		}
		
		result = new TableRow(this.filterStage.inColumns, row);
		
		return result;
	}
	
	@Override
	protected void addNextRow() throws SQLException {
		if (in.next()) {
			this.nextRows.add(buildRow());
		}
	}

	@Override
	public void close() throws SQLException {
		try {
			Closers.closeQuietly(in);
		} finally {
			this.closed = true;
		}
	}
	
	@Override
	public String getString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		throwIfBadLocation(loc);
		return this.filterStage.transform(columnIndex, this.currRow);
	}

	@Override
	public String getCursorName() throws SQLException {
		throwIfClosed(this);
		return in.getCursorName();
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.filterStage.outColumns.indexOf(columnLabel);
	}

}
