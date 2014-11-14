package gov.usgs.cida.nude;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class ResultSetUtils {
	private static final Logger log = LoggerFactory.getLogger(ResultSetUtils.class);
	
	public ResultSetUtils() {
	}
	
	public static void spitOutStuff(ResultSet actual) throws SQLException {
		while(actual.next()) {
			TableRow actualTR = TableRow.buildTableRow(actual);
			
			StringBuilder sb = new StringBuilder();
			for (Column col : actualTR.getColumns()) {
				sb.append(actualTR.getValue(col));
				sb.append(" ");
			}
			log.debug(sb.toString());
		}
		
	}
	
	public static boolean checkEqualRows(ResultSet expected, ResultSet actual) throws SQLException {
		boolean result = true;
		
		if (null != expected && null != actual) {
			ResultSetMetaData expectedMD = expected.getMetaData();
			ResultSetMetaData actualMD = actual.getMetaData();
			if (expectedMD.getColumnCount() == actualMD.getColumnCount()) {
				for (int i = 1; i <= expectedMD.getColumnCount(); i++) {
					if (!expectedMD.getColumnName(i).equals(actualMD.getColumnName(i))) {
						log.error("Non identical column names.  Expected:"+ expectedMD.getColumnName(i) +" Actual:"+ actualMD.getColumnName(i));
						result = false;
					}
				}
			} else {
				log.error("Inequal Column Count.  Expected:"+ expectedMD.getColumnCount() +" Actual:"+ actualMD.getColumnCount());
				result = false;
			}

			int rowIndex = -1;
			boolean hasRows = true;
			while(result && hasRows) {
				rowIndex++;
				boolean expectedHasRows = expected.next();
				boolean actualHasRows = actual.next();
				hasRows = expectedHasRows && actualHasRows;
				if (hasRows) {
					TableRow expectedTR = TableRow.buildTableRow(expected);
					TableRow actualTR = TableRow.buildTableRow(actual);

					if (!expectedTR.equals(actualTR)) {
						log.error("Inequal Data.  Expected:"+ expectedTR +" Actual:"+ actualTR);
						result = false;
					}
				}
			}

			if (expected.isAfterLast() != actual.isAfterLast()) {
				log.error("Sizes don't match on row " + rowIndex + ". Expected.isAfterLast:"+ expected.isAfterLast() +" Actual.isAfterLast:"+ actual.isAfterLast());
				result = false;
			}
		} else if (null == expected && null == actual) {
			log.error("You send two null ResultSets to be checked?! Uggh. I have no words. This is going on your permanent record.");
		} else {
			log.error("Dude. What kind of party you throwing? One of these are null.  Expected:"+ expected +" Actual:"+ actual);
			result = false;
		}

		return result;
	}
	
	public static Iterable<TableRow> createTableRows(ColumnGrouping cg, String[][] rows) {
		List<TableRow> result = new ArrayList<TableRow>();
		
		if (null != cg) {
			int debugIndex = -1;
			for (String[] row : rows) {
				debugIndex++;
				if (null != row && row.length == cg.size()) {
					Map<Column, String> rowMap = new HashMap<Column, String>();
					for (int j = 0; j < cg.size(); j++) {
						rowMap.put(cg.get(j+1), row[j]);
					}
					result.add(new TableRow(cg, rowMap));
				} else {
					throw new IndexOutOfBoundsException("Invalid row length at " + debugIndex);
				}
			}
		}
		
		return result;
	}
}
