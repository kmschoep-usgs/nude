package gov.usgs.cida.nude.time;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import gov.usgs.cida.nude.resultset.inmemory.IteratorWrappingResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.joda.time.DateTime;
import org.joda.time.Period;
import static org.junit.Assert.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class TimeFilteringResultSetTest {
	private static Logger log = LoggerFactory.getLogger(TimeFilteringResultSetTest.class);
	
	public TimeFilteringResultSetTest() {
	}

	protected static List<TableRow> sampleRows = null;
	protected static DateRange continuousSampleDRC = null;
	protected static List<DateTime> fullSampleTimes = null;
	
	protected static Column time = new SimpleColumn("time");
	protected static Column colA = new SimpleColumn("colA");
	protected static Column colB = new SimpleColumn("colB");
	protected static Column colC = new SimpleColumn("colC");
	protected static ColumnGrouping colGroup = new ColumnGrouping(Arrays.asList(new Column[] {time, colA, colB, colC}));
	
	protected ResultSet sampleResultSet = null;

	/**
	 * Test of sync method, of class TimeFilteringResultSet.
	 */
	@Test
	public void testAddToNextVals() {
		DateRange dummyDRC = new DateRange(fullSampleTimes.subList(5, 6), Period.minutes(10));
		TimeFilteringResultSet instance = new TimeFilteringResultSet(sampleResultSet, dummyDRC);
		
		TableRow nextInRow = null;
		
		assertEquals(0, instance.nextVals.size());
		instance.addToNextVals(nextInRow);
		assertEquals(0, instance.nextVals.size());
		
		Map<Column, String> row = new HashMap<Column, String>();
		row.put(time, "" + DateTime.parse("2012-05-10T08:00:00-05:00").getMillis());
		row.put(colA, "6a");
		row.put(colB, null);
		row.put(colC, null);
		nextInRow = new TableRow(colGroup, row);
		instance.addToNextVals(nextInRow);
		assertEquals(0, instance.nextVals.size());
		
		row.put(time, "" + DateTime.parse("2012-05-10T08:25:00-05:00").getMillis());
		row.put(colA, "6a");
		row.put(colB, null);
		row.put(colC, null);
		nextInRow = new TableRow(colGroup, row);
		instance.addToNextVals(nextInRow);
		assertEquals(1, instance.nextVals.size());
	}

	/**
	 * Test of getNextRow method, of class TimeFilteringResultSet.
	 */
	@Test
	public void testBuildNextOutRow() {
		DateRange dummyDRC = new DateRange(fullSampleTimes.subList(5, 6), Period.ZERO);
		TimeFilteringResultSet instance = new TimeFilteringResultSet(sampleResultSet, dummyDRC);
		
		Map<Column, String> row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(5).getMillis());
		TableRow expected = new TableRow(colGroup, row);
		TableRow result = instance.buildNextOutRow();
		assertNotNull(result);
		assertEquals(TimeFilteringResultSet.getPrimaryKey(expected), TimeFilteringResultSet.getPrimaryKey(result));
		result = instance.buildNextOutRow();
		assertNull(result);

		instance = new TimeFilteringResultSet(sampleResultSet, dummyDRC);
		instance.addToNextVals(sampleRows.get(0));
		result = instance.buildNextOutRow();
		assertNotNull(result);
		assertEquals(TimeFilteringResultSet.getPrimaryKey(expected), TimeFilteringResultSet.getPrimaryKey(result));
		result = instance.buildNextOutRow();
		assertNull(result);
		
		instance = new TimeFilteringResultSet(sampleResultSet, dummyDRC);
		expected = sampleRows.get(5);
		instance.addToNextVals(sampleRows.get(5));
		result = instance.buildNextOutRow();
		assertNotNull(result);
		assertEquals(TimeFilteringResultSet.getPrimaryKey(expected), TimeFilteringResultSet.getPrimaryKey(result));
		result = instance.buildNextOutRow();
		assertNull(result);
	}
	
	@Test
	public void testContinuousFullRun() {
		DateRange continuousDrc = new DateRange(fullSampleTimes.get(0), fullSampleTimes.get(fullSampleTimes.size() - 1));
		TimeFilteringResultSet in = new TimeFilteringResultSet(sampleResultSet, continuousDrc);
		try {
			for (int i = 0; i < sampleRows.size(); i++) {
				if (!in.isClosed() && in.next()) {
						TableRow expected = sampleRows.get(i);
						TableRow result = TableRow.buildTableRow(in);
						assertEquals(TimeFilteringResultSet.getPrimaryKey(expected), TimeFilteringResultSet.getPrimaryKey(result));
				} else {
					fail("apparently we have more sample rows, but in isn't ready");
				}
			}
			assertTrue(!in.isClosed());
			assertTrue(!in.next());
		} catch (SQLException ex) {
			fail("SQLException while checking");
		}
	}
	
	@Test
	public void testDiscreteFilterOneToOneRun() {
		List<Integer> indexes = Arrays.asList(new Integer[] {3, 5, 8, 9, 10, 15});
		List<DateTime> filterDates = new ArrayList<DateTime>();
		for (Integer i : indexes) {
			filterDates.add(fullSampleTimes.get(i));
		}
		DateRange discreteDrc = new DateRange(filterDates, Period.ZERO);
		TimeFilteringResultSet in = new TimeFilteringResultSet(sampleResultSet, discreteDrc);
		try {
			for (Integer i : indexes) {
				if (!in.isClosed() && in.next()) {
						TableRow expected = sampleRows.get(i);
						TableRow result = TableRow.buildTableRow(in);
						assertEquals(TimeFilteringResultSet.getPrimaryKey(expected), TimeFilteringResultSet.getPrimaryKey(result));
				} else {
					fail("apparently we have more sample rows, but in isn't ready");
				}
			}
			assertTrue(!in.isClosed());
			assertTrue(!in.next());
		} catch (SQLException ex) {
			fail("SQLException while checking");
		}
	}
	
	@Test
	public void testDiscreteFilterOneRealRowToManyOutputRowsRun() {
		List<DateTime> filterDates = new ArrayList<DateTime>();
		List<TableRow> outRows = new ArrayList<TableRow>();
		
		filterDates.add(DateTime.parse("2012-05-10T10:55:00-05:00"));
		Map<Column, String> row = new HashMap<Column, String>();
		row.put(time, "" + DateTime.parse("2012-05-10T10:55:00-05:00").getMillis());
		outRows.add(new TableRow(colGroup, row));
		
		filterDates.add(DateTime.parse("2012-05-10T11:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.putAll(sampleRows.get(14).getMap());
		row.put(time, "" + DateTime.parse("2012-05-10T11:00:00-05:00").getMillis());
		outRows.add(new TableRow(colGroup, row));
		
		filterDates.add(DateTime.parse("2012-05-10T11:10:00-05:00"));
		row = new HashMap<Column, String>();
		row.putAll(sampleRows.get(14).getMap());
		row.put(time, "" + DateTime.parse("2012-05-10T11:10:00-05:00").getMillis());
		outRows.add(new TableRow(colGroup, row));
		
		filterDates.add(DateTime.parse("2012-05-10T11:19:59-05:00"));
		row = new HashMap<Column, String>();
		row.putAll(sampleRows.get(14).getMap());
		row.put(time, "" + DateTime.parse("2012-05-10T11:19:59-05:00").getMillis());
		outRows.add(new TableRow(colGroup, row));
		
		filterDates.add(DateTime.parse("2012-05-10T11:20:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + DateTime.parse("2012-05-10T11:20:00-05:00").getMillis());
		outRows.add(new TableRow(colGroup, row));
		
		
		
		DateRange discreteDrc = new DateRange(filterDates, Period.minutes(20));
		TimeFilteringResultSet in = new TimeFilteringResultSet(sampleResultSet, discreteDrc);
		try {
			for (TableRow expected : outRows) {
				if (!in.isClosed() && in.next()) {
						TableRow result = TableRow.buildTableRow(in);
						assertEquals(TimeFilteringResultSet.getPrimaryKey(expected), TimeFilteringResultSet.getPrimaryKey(result));
				} else {
					fail("apparently we have more sample rows, but in isn't ready");
				}
			}
			assertTrue(!in.isClosed());
			assertTrue(!in.next());
		} catch (SQLException ex) {
			fail("SQLException while checking");
		}
	}
	
	@Test
	public void testDiscreteFilterManyRealRowsToOneOutputRowRun() {
		List<DateTime> filterDates = new ArrayList<DateTime>();
		List<TableRow> outRows = new ArrayList<TableRow>();
		Map<Column, String> row;
		
		filterDates.add(DateTime.parse("2012-05-10T08:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.putAll(sampleRows.get(0).getMap());
		row.put(time, "" + DateTime.parse("2012-05-10T08:00:00-05:00").getMillis());
		outRows.add(new TableRow(colGroup, row));
		
		filterDates.add(DateTime.parse("2012-05-10T08:05:00-05:00"));
		row = new HashMap<Column, String>();
		row.putAll(sampleRows.get(0).getMap());
		row.putAll(sampleRows.get(1).getMap());
		row.put(time, "" + DateTime.parse("2012-05-10T08:05:00-05:00").getMillis());
		outRows.add(new TableRow(colGroup, row));
		
		filterDates.add(DateTime.parse("2012-05-10T08:10:00-05:00"));
		row = new HashMap<Column, String>();
		row.putAll(sampleRows.get(0).getMap());
		row.putAll(sampleRows.get(1).getMap());
		row.putAll(sampleRows.get(2).getMap());
		row.put(time, "" + DateTime.parse("2012-05-10T08:10:00-05:00").getMillis());
		outRows.add(new TableRow(colGroup, row));
		
		filterDates.add(DateTime.parse("2012-05-10T08:30:00-05:00"));
		row = new HashMap<Column, String>();
		TimeFilteringResultSet.putAllNonNull(row, sampleRows.get(3).getMap());
		TimeFilteringResultSet.putAllNonNull(row, sampleRows.get(4).getMap());
		TimeFilteringResultSet.putAllNonNull(row, sampleRows.get(5).getMap());
		TimeFilteringResultSet.putAllNonNull(row, sampleRows.get(6).getMap());
		row.put(time, "" + DateTime.parse("2012-05-10T08:30:00-05:00").getMillis());
		outRows.add(new TableRow(colGroup, row));
		
		
		
		DateRange discreteDrc = new DateRange(filterDates, Period.minutes(20));
		TimeFilteringResultSet in = new TimeFilteringResultSet(sampleResultSet, discreteDrc);
		try {
			for (TableRow expected : outRows) {
				if (!in.isClosed() && in.next()) {
						TableRow result = TableRow.buildTableRow(in);
						assertEquals(TimeFilteringResultSet.getPrimaryKey(expected), TimeFilteringResultSet.getPrimaryKey(result));
						assertEquals(expected, result);
				} else {
					fail("apparently we have more sample rows, but in isn't ready");
				}
			}
			assertTrue(!in.isClosed());
			assertTrue(!in.next());
		} catch (SQLException ex) {
			fail("SQLException while checking");
		}
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
	}
	
	@Before
	public void setUp() {
		sampleResultSet = new IteratorWrappingResultSet(sampleRows.iterator());
	}
	
	@After
	public void tearDown() {
		try {
			sampleResultSet.close();
		} catch (Exception e) {
			
		}
		sampleResultSet = null;
	}
	
	@BeforeClass
	public static void setUpClass() throws Exception {
		sampleRows = new ArrayList<TableRow>();
		
		Map<Column, String> row = null;
		
		fullSampleTimes = new ArrayList<DateTime>();
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(0).getMillis());
		row.put(colA, "1a");
		row.put(colB, "1b");
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:05:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(1).getMillis());
		row.put(colA, "2a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:10:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(2).getMillis());
		row.put(colA, "3a");
		row.put(colB, null);
		row.put(colC, "0c");
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:15:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(3).getMillis());
		row.put(colA, "4a");
		row.put(colB, "2b");
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:20:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(4).getMillis());
		row.put(colA, "5a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:25:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(5).getMillis());
		row.put(colA, "6a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:30:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(6).getMillis());
		row.put(colA, "7a");
		row.put(colB, "3b");
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:35:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(7).getMillis());
		row.put(colA, "8a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:40:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(8).getMillis());
		row.put(colA, "9a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:45:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(9).getMillis());
		row.put(colA, "10a");
		row.put(colB, "4b");
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:50:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(10).getMillis());
		row.put(colA, "11a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T08:55:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(11).getMillis());
		row.put(colA, "12a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T09:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(12).getMillis());
		row.put(colA, "13a");
		row.put(colB, "5b");
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T10:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(13).getMillis());
		row.put(colA, "14a");
		row.put(colB, "6b");
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T11:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(14).getMillis());
		row.put(colA, "15a");
		row.put(colB, "7b");
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-10T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(15).getMillis());
		row.put(colA, "16a");
		row.put(colB, "8b");
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-11T00:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(16).getMillis());
		row.put(colA, "17a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-11T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(17).getMillis());
		row.put(colA, "18a");
		row.put(colB, "9b");
		row.put(colC, "1c");
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-12T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(18).getMillis());
		row.put(colA, "19a");
		row.put(colB, "10b");
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-13T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(19).getMillis());
		row.put(colA, "20a");
		row.put(colB, null);
		row.put(colC, "2c");
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-14T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(20).getMillis());
		row.put(colA, "21a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-16T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(21).getMillis());
		row.put(colA, "22a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-18T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(22).getMillis());
		row.put(colA, "23a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-05-20T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(23).getMillis());
		row.put(colA, "24a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-06-01T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(24).getMillis());
		row.put(colA, "25a");
		row.put(colB, null);
		row.put(colC, "3c");
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-06-05T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(25).getMillis());
		row.put(colA, "26a");
		row.put(colB, null);
		row.put(colC, "4c");
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-06-10T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(26).getMillis());
		row.put(colA, "27a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-06-15T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(27).getMillis());
		row.put(colA, "28a");
		row.put(colB, null);
		row.put(colC, "5c");
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-06-20T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(28).getMillis());
		row.put(colA, "29a");
		row.put(colB, null);
		row.put(colC, null);
		sampleRows.add(new TableRow(colGroup, row));
		
		fullSampleTimes.add(DateTime.parse("2012-06-25T12:00:00-05:00"));
		row = new HashMap<Column, String>();
		row.put(time, "" + fullSampleTimes.get(29).getMillis());
		row.put(colA, "30a");
		row.put(colB, null);
		row.put(colC, "6c");
		sampleRows.add(new TableRow(colGroup, row));
		
		
		continuousSampleDRC = new DateRange(fullSampleTimes.get(0), fullSampleTimes.get(fullSampleTimes.size() - 1));
	}
}
