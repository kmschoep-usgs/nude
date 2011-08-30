package examples.ida;

import gov.usgs.cida.connector.http.AbstractHttpParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IdaParser extends AbstractHttpParser<IdaTable> {
	
	protected IdaTable[] columns = IdaTable.values();
	
	protected EnumMap<IdaTable, String> row = new EnumMap<IdaTable, String>(IdaTable.class);
	
	protected static final Pattern pat = Pattern.compile("^\\s*<input.* name=\"mindatetime\".* value=\"([^\"]*)\".*\\/>.*<input.* name=\"maxdatetime\".* value=\"([^\"]*)\".*\\/>.*<input.*");
	
	@Override
	public boolean next(BufferedReader in) throws SQLException {
		this.row.clear();
		boolean result = false;
		
		try {
			boolean endOfStream = false;
			while (!result && !endOfStream) {
				String line = in.readLine();
				if (null == line) {
					endOfStream = true;
				} else {
					Matcher mat = pat.matcher(line);
					if (mat.matches() && 2 == mat.groupCount()) {
						row.put(IdaTable.MINDATETIME, mat.group(1));
						row.put(IdaTable.MAXDATETIME, mat.group(2));
						result = true;
					}
				}
			}
		} catch (IOException e) {
			throw new SQLException(e);
		}
		
		return result;
	}

	@Override
	public String getValue(int columnIndex) throws SQLException {
		String result = null;
		if (-1 < columnIndex && columnIndex < this.columns.length) {
			result = this.row.get(this.columns[columnIndex]);
		} else {
			throw new SQLException("Invalid Column");
		}
		return result;
	}

	@Override
	public Class<IdaTable> getAvailableColumns() {
		return IdaTable.class;
	}

}
