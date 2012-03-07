package gov.usgs.cida.nude.connector.http;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.cida.spec.jsl.SpecValue;

import java.sql.SQLException;

public abstract class AbstractHttpParser implements HttpParser {
	protected final ColumnGrouping cg;
	
	protected TableRow currRow = null;

	public AbstractHttpParser(ColumnGrouping cg) {
		this.cg = cg;
	}
	
	@Override
	public ColumnGrouping getAvailableColumns() {
		return cg;
	}
	
	/**
	 * 1 index based
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> SpecValue<T> getValue(Class<T> type, int columnIndex)
			throws SQLException {
		if (String.class.equals(type)) {
			return new SpecValue<T>((T) getValue(columnIndex - 1));
		} else {
			throw new SQLException("Operation not supported");
		}
	}
	
	/**
	 * 0 index based
	 * @param columnIndex
	 * @return
	 * @throws SQLException
	 */
	public String getValue(int columnIndex) throws SQLException {
		String result = null;

		if (null != currRow) {
			result = currRow.getValue(currRow.getColumns().get(columnIndex + 1));
		} else {
			throw new SQLException("No Current Row!");
		}

		return result;
	}
}
