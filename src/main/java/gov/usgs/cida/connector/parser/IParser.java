package gov.usgs.cida.connector.parser;

import gov.usgs.cida.spec.jsl.SpecValue;

import java.io.BufferedReader;
import java.sql.SQLException;

public interface IParser<Table extends Enum<Table>> {
	public boolean next(BufferedReader in) throws SQLException;
	public <T> SpecValue<T> getValue(Class<T> type, int index) throws SQLException;
	public Class<Table> getAvailableColumns();
}
