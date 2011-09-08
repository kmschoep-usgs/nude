package gov.usgs.cida.nude.resultset;

import gov.usgs.cida.nude.table.ColumnGrouping;
import gov.usgs.cida.nude.values.TableRow;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuxResultSet extends StringValImplResultSet implements ColumnGroupedResultSet {
	private static final Logger log = LoggerFactory
			.getLogger(MuxResultSet.class);
	protected boolean isClosed;
	
	protected final List<ColumnGroupedResultSet> rsets;
	protected final List<TableRow> nextRsetRows;
	protected final ResultSetMetaData metadata;
	
	protected final ColumnGrouping columns;
	
	protected TableRow currRow;
	protected final Queue<TableRow> nextRows;
	
	public MuxResultSet(List<ColumnGroupedResultSet> inputs) {
		this.isClosed = false;
		
		if (null == inputs) {
			inputs = new ArrayList<ColumnGroupedResultSet>();
		}
		this.rsets = inputs;
		this.nextRsetRows = new ArrayList<TableRow>();
		
		ColumnGrouping columns = null;
		for (ColumnGroupedResultSet rs : inputs) {
			ColumnGrouping cg = rs.getColumnGrouping(); 
			if (null == columns) {
				columns = new ColumnGrouping(cg.getPrimaryKey());
			}
			columns = columns.join(cg);
		}
		this.columns = columns;
		
		this.metadata = new CGResultSetMetaData(this.columns);
		
		this.currRow = null;
		this.nextRows = new LinkedList<TableRow>();
	}
	
	@Override
	public boolean next() throws SQLException {
		throwIfClosed(this);
		
		//DOOOOOOOOOOOOOoosomething!
		
		return false;
	}

	@Override
	public void close() throws SQLException {
		this.isClosed = true;
		for (ResultSet rs : this.rsets) {
			try {
				rs.close();
			} catch (Exception e) {
				log.error("Exception closing muxed ResultSet", e);
			}
		}
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		throwIfClosed(this);
		return this.currRow.getValue(this.columns.get(columnIndex));
	}

	@Override
	public String getCursorName() throws SQLException {
		throwIfClosed(this);
		return "" + this.hashCode();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throwIfClosed(this);
		return this.metadata;
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		throwIfClosed(this);
		return this.columns.indexOf(columnLabel);
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.isClosed;
	}

	@Override
	public ColumnGrouping getColumnGrouping() {
		return this.columns;
	}

}
