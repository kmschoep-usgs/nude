package gov.usgs.cida.nude.filter;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import examples.ida.response.ClientData;
import examples.ida.response.IdaData;
import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.DummyColumn;
import gov.usgs.cida.nude.resultset.inmemory.StringTableResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterTest {
	private static final Logger log = LoggerFactory
			.getLogger(FilterTest.class);
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		List<Column> cols = new ArrayList<Column>();
		cols.addAll(Arrays.asList(IdaData.values()));
		this.inColGroup = new ColumnGrouping(IdaData.date_time, cols);
		
		cols = new ArrayList<Column>();
		cols.addAll(Arrays.asList(ClientData.values()));
		this.outColGroup = new ColumnGrouping(ClientData.timestamp, cols);
		
		this.muxCg = new ColumnGrouping(IdaData.date_time, Arrays.asList(new Column[] {IdaData.date_time, DummyColumn.JOIN})); 
		
		this.muxOutCg = this.outColGroup.join(new ColumnGrouping(ClientData.timestamp, Arrays.asList(new Column[] {ClientData.timestamp, DummyColumn.JOIN})));
	}
	
	@After
	public void tearDown() throws Exception {
		this.inColGroup = null;
		this.outColGroup = null;
	}
	
	protected ColumnGrouping inColGroup;
	protected ColumnGrouping outColGroup;
	protected ColumnGrouping muxCg;
	protected ColumnGrouping muxOutCg;
	
	@Test
	public void testFilteringResults() throws Exception {
		ResultSet input = buildInputResultSet();
		ResultSet exOut = buildExpectedOutput();
		NudeFilter filter = new NudeFilter();
		
		FilterStageBuilder gb = new FilterStageBuilder(inColGroup);
		
		gb.addTransform(ClientData.timestamp, new ColumnAlias(IdaData.date_time));
		gb.addTransform(ClientData.value, new ColumnAlias(IdaData.value));
		
		filter.addFilterStage(gb.buildFilterStage());
		
		gb = new FilterStageBuilder(outColGroup);
		
		filter.addFilterStage(gb.buildFilterStage());
		
		List<ResultSet> inputs = new ArrayList<ResultSet>();
		inputs.add(input);
		
		ResultSet output = filter.filter(inputs);
		
		assertNotNull(output);
		
		assertTrue(exOut.isBeforeFirst());
		assertTrue(output.isBeforeFirst());
		
		assertTrue(exOut.next());
		assertTrue(output.next());
		
		assertTrue(exOut.isFirst());
		assertTrue(output.isFirst());
		
		while (!exOut.isAfterLast() && !output.isAfterLast()) {
			checkRowEquality(exOut,output);
			exOut.next();
			output.next();
		}
		
		if (!exOut.isAfterLast() || !output.isAfterLast()) {
			assertTrue("Row count unequal", false);
		}
	}
	
	@Test
	public void testMuxFiltering() throws SQLException {
		ResultSet input = buildInputResultSet();
		ResultSet muxIn = buildMuxTestResultSet();
		ResultSet exOut = buildMuxOut();
		NudeFilter filter = new NudeFilter();
		
		FilterStageBuilder gb = new FilterStageBuilder(inColGroup.join(muxCg));
		
		gb.addTransform(ClientData.timestamp, new ColumnAlias(IdaData.date_time));
		gb.addTransform(ClientData.value, new ColumnAlias(IdaData.value));
		
		filter.addFilterStage(gb.buildFilterStage());
		
		gb = new FilterStageBuilder(this.muxOutCg);
		
		filter.addFilterStage(gb.buildFilterStage());
		
		List<ResultSet> inputs = new ArrayList<ResultSet>();
		inputs.add(input);
		inputs.add(muxIn);
		
		ResultSet output = filter.filter(inputs);
		
		assertNotNull(output);
		
		assertTrue(exOut.isBeforeFirst());
		assertTrue(output.isBeforeFirst());
		
		assertTrue(exOut.next());
		assertTrue(output.next());
		
		assertTrue(exOut.isFirst());
		assertTrue(output.isFirst());
		
		while (!exOut.isAfterLast() && !output.isAfterLast()) {
			checkRowEquality(exOut,output);
			exOut.next();
			output.next();
		}
		
		if (!exOut.isAfterLast() || !output.isAfterLast()) {
			assertTrue("Row count unequal", false);
		}
	}
	
	public static boolean checkRowEquality(ResultSet expected, ResultSet actual) throws SQLException {
		boolean result = false;
		int exCnt = expected.getMetaData().getColumnCount();
		int acCnt = actual.getMetaData().getColumnCount();
		
		if (exCnt == acCnt) {
			for (int i = 1; i <= exCnt; i++) {
				String colLabel = expected.getMetaData().getColumnName(i);
				
				String exStr = expected.getString(colLabel);
				String acStr = actual.getString(colLabel);
				
				log.trace(exStr + " : " + acStr);
				assertEquals(exStr, acStr);
			}
		} else {
			assertTrue(false);
		}
		return result;
	}
	
	public ResultSet buildExpectedOutput() {
		ResultSet result = null;
		
		StringTableResultSet rs = new StringTableResultSet(this.outColGroup);
		// fill the set
		Map<Column, String> row = null;
		for (int i = 0; i < dates.length && i < values.length; i++) {
			row = new HashMap<Column, String>();
			row.put(ClientData.timestamp, dates[i]);
			row.put(ClientData.value, values[i]);
			rs.addRow(new TableRow(this.outColGroup, row));
		}
		
		result = rs;
		return result;
	}
	
	public ResultSet buildInputResultSet() {
		ResultSet result = null;
		
		StringTableResultSet rs = new StringTableResultSet(this.inColGroup);
		// fill the set
		Map<Column, String> row = null;
		
		for (int i = 0; i < dates.length && i < values.length; i++) {
			row = new HashMap<Column, String>();
			row.put(IdaData.site_no, "04085427");
			row.put(IdaData.date_time, dates[i]);
			row.put(IdaData.tz_cd, "CDT");
			row.put(IdaData.dd, "2");
			row.put(IdaData.accuracy_cd, "1");
			row.put(IdaData.value, values[i]);
			row.put(IdaData.precision, "3");
			row.put(IdaData.remark, null);
			rs.addRow(new TableRow(this.inColGroup, row));
		}
		
		result = rs;
		return result;
	}
	
	public ResultSet buildMuxTestResultSet() {
		ResultSet result = null;
		
		StringTableResultSet rs = new StringTableResultSet(this.muxCg);
		// fill the set
		Map<Column, String> row = null;
		
		for (int i = 0; i < dates.length && i < values.length; i++) {
			row = new HashMap<Column, String>();
			row.put(IdaData.date_time, dates[i]);
			row.put(DummyColumn.JOIN, "" + i);
			rs.addRow(new TableRow(this.muxCg, row));
		}
		
		result = rs;
		return result;
	}
	
	public ResultSet buildMuxOut() {
		ResultSet result = null;
		
		StringTableResultSet rs = new StringTableResultSet(this.muxOutCg);
		// fill the set
		Map<Column, String> row = null;
		for (int i = 0; i < dates.length && i < values.length; i++) {
			row = new HashMap<Column, String>();
			row.put(DummyColumn.JOIN, "" + i);
			row.put(ClientData.timestamp, dates[i]);
			row.put(ClientData.value, values[i]);
			rs.addRow(new TableRow(this.muxOutCg, row));
		}
		
		result = rs;
		return result;
	}
	
	protected String[] dates = new String[] {
			"20100830000000",
			"20100830001500",
			"20100830003000",
			"20100830004500",
			"20100830010000",
			"20100830011500",
			"20100830013000",
			"20100830014500",
			"20100830020000",
			"20100830021500",
			"20100830023000",
			"20100830024500",
			"20100830030000",
			"20100830031500",
			"20100830033000",
			"20100830034500",
			"20100830040000",
			"20100830041500",
			"20100830043000",
			"20100830044500",
			"20100830050000",
			"20100830051500",
			"20100830053000",
			"20100830054500",
			"20100830060000",
			"20100830061500",
			"20100830063000",
			"20100830064500",
			"20100830070000",
			"20100830071500",
			"20100830073000",
			"20100830074500",
			"20100830080000",
			"20100830081500",
			"20100830083000",
			"20100830084500",
			"20100830090000",
			"20100830091500",
			"20100830093000",
			"20100830094500",
			"20100830100000",
			"20100830101500",
			"20100830103000",
			"20100830104500",
			"20100830110000",
			"20100830111500",
			"20100830113000",
			"20100830114500",
			"20100830120000",
			"20100830121500",
			"20100830123000",
			"20100830124500",
			"20100830130000",
			"20100830131500",
			"20100830133000",
			"20100830134500",
			"20100830140000",
			"20100830141500",
			"20100830143000",
			"20100830144500",
			"20100830150000",
			"20100830151500",
			"20100830153000",
			"20100830154500",
			"20100830160000",
			"20100830161500",
			"20100830163000",
			"20100830164500",
			"20100830170000",
			"20100830171500",
			"20100830173000",
			"20100830174500",
			"20100830180000",
			"20100830181500",
			"20100830183000",
			"20100830184500",
			"20100830190000",
			"20100830191500",
			"20100830193000",
			"20100830194500",
			"20100830200000",
			"20100830201500",
			"20100830203000",
			"20100830204500",
			"20100830210000",
			"20100830211500",
			"20100830213000",
			"20100830214500",
			"20100830220000",
			"20100830221500",
			"20100830223000",
			"20100830224500",
			"20100830230000",
			"20100830231500",
			"20100830233000",
			"20100830234500",
			"20100831000000"
	};
	
	protected String[] values = new String[] {
			"118",
			"113",
			"113",
			"115",
			"113",
			"118",
			"113",
			"113",
			"118",
			"115",
			"115",
			"115",
			"113",
			"115",
			"115",
			"111",
			"115",
			"111",
			"115",
			"115",
			"111",
			"111",
			"115",
			"113",
			"115",
			"115",
			"115",
			"111",
			"118",
			"113",
			"113",
			"111",
			"111",
			"105",
			"111",
			"111",
			"111",
			"107",
			"107",
			"107",
			"105",
			"107",
			"105",
			"111",
			"107",
			"105",
			"105",
			"105",
			"111",
			"107",
			"107",
			"107",
			"105",
			"107",
			"103",
			"103",
			"101",
			"107",
			"105",
			"105",
			"105",
			"105",
			"105",
			"101",
			"105",
			"103",
			"103",
			"105",
			"101",
			"103",
			"105",
			"99",
			"103",
			"103",
			"105",
			"101",
			"103",
			"101",
			"101",
			"101",
			"99",
			"103",
			"103",
			"103",
			"101",
			"103",
			"103",
			"101",
			"101",
			"101",
			"101",
			"99",
			"103",
			"99",
			"99",
			"101",
			"103"
	};
}
