package gov.usgs.cida.nude.connector.http;

import gov.usgs.cida.spec.jsl.SpecValue;

import java.sql.SQLException;

public abstract class AbstractHttpParser implements HttpParser {
	
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
	public abstract String getValue(int columnIndex) throws SQLException;
}
