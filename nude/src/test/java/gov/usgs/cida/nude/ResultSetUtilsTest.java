package gov.usgs.cida.nude;

import static gov.usgs.cida.nude.ResultSetUtils.checkEqualRows;
import static gov.usgs.cida.nude.ResultSetUtils.createTableRows;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.resultset.inmemory.IteratorWrappingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class ResultSetUtilsTest {
	private static final Logger log = LoggerFactory.getLogger(ResultSetUtilsTest.class);

	public ResultSetUtilsTest() {
	}
	
	@Test
	public void sameDatasetsAreEqualTest() throws SQLException {
		ResultSet actual = new IteratorWrappingResultSet(kitchenSinkDataset.iterator());
		ResultSet expected = new IteratorWrappingResultSet(kitchenSinkDataset.iterator());
		
		assertTrue(checkEqualRows(expected, actual));
	}
	
	@Test
	public void identicalDatasetsAreEqualTest() throws SQLException {
		ResultSet actual = new IteratorWrappingResultSet(kitchenSinkDataset.iterator());
		ResultSet expected = new IteratorWrappingResultSet(identicalSinkDataset.iterator());
		
		assertTrue(checkEqualRows(expected, actual));
	}
	
	@Test
	public void differentNumColumnsIsNotEqualTest() throws SQLException {
		ResultSet actual = new IteratorWrappingResultSet(kitchenSinkDataset.iterator());
		ResultSet expected = new IteratorWrappingResultSet(thinSinkDataset.iterator());
		
		assertTrue(!checkEqualRows(expected, actual));
	}
	
	@Test
	public void differentNamedColumnsIsNotEqualTest() throws SQLException {
		ResultSet actual = new IteratorWrappingResultSet(kitchenSinkDataset.iterator());
		ResultSet expected = new IteratorWrappingResultSet(thinkSinkDataset.iterator());
		
		assertTrue(!checkEqualRows(expected, actual));
	}
	
	@Test
	public void differentDataIsNotEqualTest() throws SQLException {
		ResultSet actual = new IteratorWrappingResultSet(kitchenSinkDataset.iterator());
		ResultSet expected = new IteratorWrappingResultSet(differentSinkDataset.iterator());
		
		assertTrue(!checkEqualRows(expected, actual));
	}
	
	@Test
	public void differentNumRowsIsNotEqualTest() throws SQLException {
		ResultSet actual = new IteratorWrappingResultSet(kitchenSinkDataset.iterator());
		ResultSet expected = new IteratorWrappingResultSet(filteredSinkDataset.iterator());
		
		assertTrue(!checkEqualRows(expected, actual));
	}
	
	protected static ColumnGrouping kitchenSinkColGroup = null;
	protected static Iterable<TableRow> kitchenSinkDataset = null;
	protected static Iterable<TableRow> identicalSinkDataset = null;
	protected static Iterable<TableRow> thinSinkDataset = null;
	protected static Iterable<TableRow> thinkSinkDataset = null;
	protected static Iterable<TableRow> differentSinkDataset = null;
	protected static Iterable<TableRow> filteredSinkDataset = null;
	
	@BeforeClass
	public static void setUpClass() throws Exception {
		log.info("Don't worry about logs here.");
		kitchenSinkColGroup = new ColumnGrouping(Arrays.asList(new Column[] {
			new SimpleColumn("time"), 
			new SimpleColumn("00060"), 
			new SimpleColumn("00010"), 
			new SimpleColumn("63680"), 
			new SimpleColumn("80222"), 
			new SimpleColumn("lab80222"), 
			new SimpleColumn("edi80222"), 
			new SimpleColumn("ewi80222"), 
			new SimpleColumn("100200"), 
			new SimpleColumn("lab100200"), 
			new SimpleColumn("edi100200"), 
			new SimpleColumn("ewi100200")}));
		kitchenSinkDataset = createTableRows(
				kitchenSinkColGroup,
				new String[][] {
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:00:00").getMillis(), null, "7.97", "73.4", "103", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:14:00").getMillis(), "9410", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:15:00").getMillis(), null, "7.97", "73.5", "79.2", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:29:00").getMillis(), "9415", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:30:00").getMillis(), null, "8", "68.6", "90.1", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:44:00").getMillis(), "9438", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:45:00").getMillis(), null, "8", "62.8", "120", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:59:00").getMillis(), "9529", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:00:00").getMillis(), null, "8.02", "70.3", "72.3", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:14:00").getMillis(), "9634", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:15:00").getMillis(), null, "8.05", "60.7", "94.9", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:29:00").getMillis(), "9758", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:30:00").getMillis(), null, "8.05", "63.4", "78.3", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:44:00").getMillis(), "9951", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:45:00").getMillis(), null, "8.07", "53.8", "95.5", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:59:00").getMillis(), "10184", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:00:00").getMillis(), null, "8.1", "56.5", "78.4", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:14:00").getMillis(), "10425", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:15:00").getMillis(), null, "8.1", "54.9", "76.8", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:29:00").getMillis(), "10668", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:30:00").getMillis(), null, "8.12", "51.6", "83", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:44:00").getMillis(), "10994", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:45:00").getMillis(), null, "8.15", "48.8", "76.6", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:59:00").getMillis(), "11242", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:00:00").getMillis(), null, "8.17", "45.7", "113", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:14:00").getMillis(), "11580", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:15:00").getMillis(), null, "8.2", "45.2", "92.8", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:29:00").getMillis(), "11875", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:30:00").getMillis(), null, "8.22", "46", "110", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:44:00").getMillis(), "12079", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:45:00").getMillis(), null, "8.22", "48", "120", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:59:00").getMillis(), "12279", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:00:00").getMillis(), null, "8.25", "42.9", "101", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:14:00").getMillis(), "12491", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:15:00").getMillis(), null, "8.25", "37.4", "75.5", null, null, null, ".14", null, null, null}
				});
		identicalSinkDataset = createTableRows(
				kitchenSinkColGroup,
				new String[][] {
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:00:00").getMillis(), null, "7.97", "73.4", "103", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:14:00").getMillis(), "9410", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:15:00").getMillis(), null, "7.97", "73.5", "79.2", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:29:00").getMillis(), "9415", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:30:00").getMillis(), null, "8", "68.6", "90.1", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:44:00").getMillis(), "9438", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:45:00").getMillis(), null, "8", "62.8", "120", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:59:00").getMillis(), "9529", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:00:00").getMillis(), null, "8.02", "70.3", "72.3", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:14:00").getMillis(), "9634", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:15:00").getMillis(), null, "8.05", "60.7", "94.9", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:29:00").getMillis(), "9758", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:30:00").getMillis(), null, "8.05", "63.4", "78.3", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:44:00").getMillis(), "9951", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:45:00").getMillis(), null, "8.07", "53.8", "95.5", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:59:00").getMillis(), "10184", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:00:00").getMillis(), null, "8.1", "56.5", "78.4", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:14:00").getMillis(), "10425", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:15:00").getMillis(), null, "8.1", "54.9", "76.8", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:29:00").getMillis(), "10668", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:30:00").getMillis(), null, "8.12", "51.6", "83", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:44:00").getMillis(), "10994", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:45:00").getMillis(), null, "8.15", "48.8", "76.6", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:59:00").getMillis(), "11242", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:00:00").getMillis(), null, "8.17", "45.7", "113", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:14:00").getMillis(), "11580", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:15:00").getMillis(), null, "8.2", "45.2", "92.8", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:29:00").getMillis(), "11875", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:30:00").getMillis(), null, "8.22", "46", "110", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:44:00").getMillis(), "12079", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:45:00").getMillis(), null, "8.22", "48", "120", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:59:00").getMillis(), "12279", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:00:00").getMillis(), null, "8.25", "42.9", "101", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:14:00").getMillis(), "12491", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:15:00").getMillis(), null, "8.25", "37.4", "75.5", null, null, null, ".14", null, null, null}
				});
		ColumnGrouping thinColGroup = new ColumnGrouping(Arrays.asList(new Column[] {
			new SimpleColumn("time"), 
			new SimpleColumn("00060"), 
			new SimpleColumn("00010"), 
			new SimpleColumn("63680"), 
			new SimpleColumn("80222"), 
			new SimpleColumn("lab80222"), 
			new SimpleColumn("edi80222"), 
			new SimpleColumn("ewi80222"), 
			new SimpleColumn("100200"), 
			new SimpleColumn("lab100200"), 
			new SimpleColumn("edi100200")
		}));
		thinSinkDataset = createTableRows(
				thinColGroup,
				new String[][] {
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:00:00").getMillis(), null, "7.97", "73.4", "103", null, null, null, ".13", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:14:00").getMillis(), "9410", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:15:00").getMillis(), null, "7.97", "73.5", "79.2", null, null, null, ".14", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:29:00").getMillis(), "9415", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:30:00").getMillis(), null, "8", "68.6", "90.1", null, null, null, ".13", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:44:00").getMillis(), "9438", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:45:00").getMillis(), null, "8", "62.8", "120", null, null, null, ".13", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:59:00").getMillis(), "9529", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:00:00").getMillis(), null, "8.02", "70.3", "72.3", null, null, null, ".14", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:14:00").getMillis(), "9634", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:15:00").getMillis(), null, "8.05", "60.7", "94.9", null, null, null, ".13", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:29:00").getMillis(), "9758", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:30:00").getMillis(), null, "8.05", "63.4", "78.3", null, null, null, ".14", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:44:00").getMillis(), "9951", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:45:00").getMillis(), null, "8.07", "53.8", "95.5", null, null, null, ".14", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:59:00").getMillis(), "10184", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:00:00").getMillis(), null, "8.1", "56.5", "78.4", null, null, null, ".14", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:14:00").getMillis(), "10425", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:15:00").getMillis(), null, "8.1", "54.9", "76.8", null, null, null, ".13", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:29:00").getMillis(), "10668", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:30:00").getMillis(), null, "8.12", "51.6", "83", null, null, null, ".14", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:44:00").getMillis(), "10994", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:45:00").getMillis(), null, "8.15", "48.8", "76.6", null, null, null, ".14", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:59:00").getMillis(), "11242", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:00:00").getMillis(), null, "8.17", "45.7", "113", null, null, null, ".14", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:14:00").getMillis(), "11580", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:15:00").getMillis(), null, "8.2", "45.2", "92.8", null, null, null, ".13", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:29:00").getMillis(), "11875", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:30:00").getMillis(), null, "8.22", "46", "110", null, null, null, ".14", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:44:00").getMillis(), "12079", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:45:00").getMillis(), null, "8.22", "48", "120", null, null, null, ".13", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:59:00").getMillis(), "12279", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:00:00").getMillis(), null, "8.25", "42.9", "101", null, null, null, ".13", null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:14:00").getMillis(), "12491", null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:15:00").getMillis(), null, "8.25", "37.4", "75.5", null, null, null, ".14", null, null}
				});
		ColumnGrouping thinkColGroup = new ColumnGrouping(Arrays.asList(new Column[] {
			new SimpleColumn("time"), 
			new SimpleColumn("00060"), 
			new SimpleColumn("00010"), 
			new SimpleColumn("63680"), 
			new SimpleColumn("80222"), 
			new SimpleColumn("lab80222"), 
			new SimpleColumn("edi80222"), 
			new SimpleColumn("ewi80222"), 
			new SimpleColumn("100300"), 
			new SimpleColumn("lab100200"), 
			new SimpleColumn("edi100200"), 
			new SimpleColumn("ewi100200")
		}));
		thinkSinkDataset = createTableRows(
				thinkColGroup,
				new String[][] {
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:00:00").getMillis(), null, "7.97", "73.4", "103", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:14:00").getMillis(), "9410", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:15:00").getMillis(), null, "7.97", "73.5", "79.2", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:29:00").getMillis(), "9415", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:30:00").getMillis(), null, "8", "68.6", "90.1", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:44:00").getMillis(), "9438", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:45:00").getMillis(), null, "8", "62.8", "120", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:59:00").getMillis(), "9529", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:00:00").getMillis(), null, "8.02", "70.3", "72.3", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:14:00").getMillis(), "9634", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:15:00").getMillis(), null, "8.05", "60.7", "94.9", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:29:00").getMillis(), "9758", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:30:00").getMillis(), null, "8.05", "63.4", "78.3", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:44:00").getMillis(), "9951", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:45:00").getMillis(), null, "8.07", "53.8", "95.5", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:59:00").getMillis(), "10184", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:00:00").getMillis(), null, "8.1", "56.5", "78.4", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:14:00").getMillis(), "10425", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:15:00").getMillis(), null, "8.1", "54.9", "76.8", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:29:00").getMillis(), "10668", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:30:00").getMillis(), null, "8.12", "51.6", "83", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:44:00").getMillis(), "10994", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:45:00").getMillis(), null, "8.15", "48.8", "76.6", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:59:00").getMillis(), "11242", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:00:00").getMillis(), null, "8.17", "45.7", "113", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:14:00").getMillis(), "11580", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:15:00").getMillis(), null, "8.2", "45.2", "92.8", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:29:00").getMillis(), "11875", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:30:00").getMillis(), null, "8.22", "46", "110", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:44:00").getMillis(), "12079", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:45:00").getMillis(), null, "8.22", "48", "120", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:59:00").getMillis(), "12279", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:00:00").getMillis(), null, "8.25", "42.9", "101", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:14:00").getMillis(), "12491", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:15:00").getMillis(), null, "8.25", "37.4", "75.5", null, null, null, ".14", null, null, null}
				});
		differentSinkDataset = createTableRows(
				kitchenSinkColGroup,
				new String[][] {
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:00:00").getMillis(), null, "7.97", "73.4", "103", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:14:00").getMillis(), "9410", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:15:00").getMillis(), null, "7.97", "73.5", "79.2", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:29:00").getMillis(), "9415", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:30:00").getMillis(), null, "8", "68.6", "90.1", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:44:00").getMillis(), "9438", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:45:00").getMillis(), null, "8", "62.8", "120", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:59:00").getMillis(), "9529", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:00:00").getMillis(), null, "8.02", "70.3", "72.3", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:14:00").getMillis(), "9634", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:15:00").getMillis(), null, "8.05", "60.7", "94.9", null, "94.9", null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:29:00").getMillis(), "9758", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:30:00").getMillis(), null, "8.05", "63.4", "78.3", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:44:00").getMillis(), "9951", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:45:00").getMillis(), null, "8.07", "53.8", "95.5", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:59:00").getMillis(), "10184", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:00:00").getMillis(), null, "8.1", "56.5", "78.4", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:14:00").getMillis(), "10425", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:15:00").getMillis(), null, "8.1", "54.9", "76.8", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:29:00").getMillis(), "10668", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:30:00").getMillis(), null, "8.12", "51.6", "83", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:44:00").getMillis(), "10994", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:45:00").getMillis(), null, "8.15", "48.8", "76.6", null, "76.6", null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:59:00").getMillis(), "11242", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:00:00").getMillis(), null, "8.17", "45.7", "113", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:14:00").getMillis(), "11580", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:15:00").getMillis(), null, "8.2", "45.2", null, null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:29:00").getMillis(), "11875", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:30:00").getMillis(), null, "8.22", "46", "110", null, null, null, ".14", null, "110", null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:44:00").getMillis(), "12079", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:45:00").getMillis(), null, "8.22", "48", "120", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T03:59:00").getMillis(), "12279", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:00:00").getMillis(), null, "8.25", "42.9", "101", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:14:00").getMillis(), "12491", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T04:15:00").getMillis(), null, "8.25", "37.4", "75.5", null, null, null, ".14", null, null, null}
				});
		filteredSinkDataset = createTableRows(
				kitchenSinkColGroup,
				new String[][] {
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:00:00").getMillis(), null, "7.97", "73.4", "103", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:14:00").getMillis(), "9410", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:15:00").getMillis(), null, "7.97", "73.5", "79.2", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:29:00").getMillis(), "9415", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:30:00").getMillis(), null, "8", "68.6", "90.1", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:44:00").getMillis(), "9438", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:45:00").getMillis(), null, "8", "62.8", "120", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T00:59:00").getMillis(), "9529", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:00:00").getMillis(), null, "8.02", "70.3", "72.3", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:14:00").getMillis(), "9634", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:15:00").getMillis(), null, "8.05", "60.7", "94.9", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:29:00").getMillis(), "9758", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:30:00").getMillis(), null, "8.05", "63.4", "78.3", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:44:00").getMillis(), "9951", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:45:00").getMillis(), null, "8.07", "53.8", "95.5", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T01:59:00").getMillis(), "10184", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:00:00").getMillis(), null, "8.1", "56.5", "78.4", null, null, null, ".14", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:14:00").getMillis(), "10425", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:15:00").getMillis(), null, "8.1", "54.9", "76.8", null, null, null, ".13", null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:29:00").getMillis(), "10668", null, null, null, null, null, null, null, null, null, null},
					new String[] {"" + ISODateTimeFormat.localDateOptionalTimeParser().parseDateTime("2008-03-01T02:30:00").getMillis(), null, "8.12", "51.6", "83", null, null, null, ".14", null, null, null}
				});

	}

	@AfterClass
	public static void tearDownClass() throws Exception {
		log.info("End your apathy about logs and start looking out for trouble!");
		kitchenSinkDataset = null;
	}
	
	@Before
	public void setUp() {
	}
	
	@After
	public void tearDown() {
	}
}
