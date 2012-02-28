package gov.usgs.cida.nude.connector.parser;

import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.spec.jsl.SpecValue;

import java.io.Reader;
import java.sql.SQLException;

public interface IParser {
	public boolean next(Reader in) throws SQLException;
	public <T> SpecValue<T> getValue(Class<T> type, int index) throws SQLException;
	public ColumnGrouping getAvailableColumns();
}
