package gov.usgs.cida.nude.resultset.inmemory;

import gov.usgs.cida.nude.column.CGResultSetMetaData;
import gov.usgs.cida.nude.resultset.CursorLocation.Location;
import gov.usgs.cida.nude.resultset.PeekingResultSet;

import java.sql.SQLException;
import java.util.Iterator;

public class IteratorWrappingResultSet extends PeekingResultSet {
	
	protected final Iterator<TableRow> it;
	
	public IteratorWrappingResultSet(Iterator<TableRow> rows) {
		this.closed = false;
		
		if (null != rows && rows.hasNext()) {
			this.it = rows;
			try {
				this.addNextRow();
				TableRow next = this.nextRows.peek();
				if (null != next) {
					this.columns = next.getColumns();
					this.metadata = new CGResultSetMetaData(this.columns);
					this.loc.setLocation(Location.BEFOREFIRST);
				} else {
					this.closed = true;
					this.loc.setLocation(Location.AFTERLAST);
				}
			} catch (Exception E) {
				this.closed = true;
				this.loc.setLocation(Location.AFTERLAST);
			}
		} else {
			this.closed = true;
			this.it = null;
			this.loc.setLocation(Location.AFTERLAST);
		}
		
	}

	@Override
	public String getCursorName() throws SQLException {
		throwIfClosed(this);
		return "" + this.it.hashCode();
	}

	@Override
	protected void addNextRow() throws SQLException {
		if (this.it.hasNext()) {
			TableRow next = this.it.next();
			this.nextRows.add(next);
		}
	}

}
