package gov.usgs.cida.nude.resultset;

import gov.usgs.cida.nude.resultset.CursorLocation.Location;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;
import gov.usgs.cida.nude.values.TableRow;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuxResultSet extends StringValImplResultSet implements ColumnGroupedResultSet {
	private static final Logger log = LoggerFactory
			.getLogger(MuxResultSet.class);
	protected boolean isClosed;
		
	protected final Map<ColumnGroupedResultSet, TableRow> rsetRows;
	
	protected final ResultSetMetaData metadata;
	
	protected final ColumnGrouping columns;
	
	protected TableRow currRow;
	protected final Queue<TableRow> nextRows;
	
	public MuxResultSet(Collection<ColumnGroupedResultSet> inputs) {
		this.isClosed = false;
		
		if (null == inputs) {
			inputs = new ArrayList<ColumnGroupedResultSet>();
		}
		
		this.rsetRows = new HashMap<ColumnGroupedResultSet, TableRow>();
		for (ColumnGroupedResultSet rs : inputs) {
			this.rsetRows.put(rs, null);
		}
		
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

	protected void addNextRow() throws SQLException {
		TableRow result = null;
		Map<Column, String> row = new HashMap<Column, String>();
		
		List<Entry<ColumnGroupedResultSet, TableRow>> usedEntries = new ArrayList<Entry<ColumnGroupedResultSet, TableRow>>();
		TableRow minPrimaryKey = null;
		for (Entry<ColumnGroupedResultSet, TableRow> rsTr : this.rsetRows.entrySet()) {
			ColumnGroupedResultSet rs = rsTr.getKey();
			TableRow tr = rsTr.getValue();
			
			if (null == tr) { //Build next row
				if (!rs.isClosed() && rs.next()) {
					tr = buildRow(rs);
					rsTr.setValue(tr);
				}
			}
			
			if (null != tr) {
				if (null == minPrimaryKey) {
					minPrimaryKey = tr;
					usedEntries.add(rsTr);
				} else {
					int negIfBetter = tr.compareTo(minPrimaryKey);
					if (0 > negIfBetter) {
						// replace current row
						minPrimaryKey = tr;
						usedEntries.clear();
						usedEntries.add(rsTr);
					} else if (0 == negIfBetter) {
						// add to current row
						usedEntries.add(rsTr);
					} else if (0 < negIfBetter) {
						// Skip
					}
				}
			}
		}
		
		for (Entry<ColumnGroupedResultSet, TableRow> ent : usedEntries) {
			row.putAll(ent.getValue().getMap());
			ent.setValue(null);
		}
		
		if (0 < row.size()) {
			result = new TableRow(this.columns, row);
			this.nextRows.add(result);
		}
	}
	
	protected static TableRow buildRow(ColumnGroupedResultSet rs) throws SQLException {
		TableRow result = null;
		Map<Column, String> row = new HashMap<Column, String>();
		
		for (Column col : rs.getColumnGrouping()) {
			row.put(col, rs.getString(col.getName()));
		}
		
		result = new TableRow(rs.getColumnGrouping(), row);
		
		return result;
	}
	
	@Override
	public void close() throws SQLException {
		this.isClosed = true;
		for (ResultSet rs : this.rsetRows.keySet()) {
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
