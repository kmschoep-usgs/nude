package gov.usgs.cida.nude.gel;


import static org.junit.Assert.*;
import examples.ida.response.ClientData;
import examples.ida.response.IdaData;
import gov.usgs.cida.nude.gel.transforms.GelTransform;
import gov.usgs.cida.nude.resultset.StringTableResultSet;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GelStackTest {

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
		
		this.input = buildInputResultSet();
		
		this.exOut = buildExpectedOutput();
	}
	
	public ResultSet buildInputResultSet() {
		ResultSet result = null;
		
		StringTableResultSet rs = new StringTableResultSet(this.inColGroup);
		
		//TODO fill the set
		
		result = rs;
		return result;
	}
	
	public ResultSet buildExpectedOutput() {
		ResultSet result = null;
		
		StringTableResultSet rs = new StringTableResultSet(this.outColGroup);
		//TODO fill the set
		
		result = rs;
		return result;
	}
	
	@After
	public void tearDown() throws Exception {
		this.input = null;
		this.inColGroup = null;
		this.outColGroup = null;
	}
	
	ResultSet input;
	ResultSet exOut;
	ColumnGrouping inColGroup;
	ColumnGrouping outColGroup;
	
	@Test
	public void testGellingResults() throws Exception {
		GelStack gelStack = new GelStack();
		
		GelBuilder gb = new GelBuilder(inColGroup);
		
		gb.addGelTransform(ClientData.timestamp, new GelTransform(IdaData.date_time));
		gb.addGelTransform(ClientData.value, new GelTransform(IdaData.value));
		
		gelStack.addGel(gb.buildGel());
		
		gb = new GelBuilder(outColGroup);
		
		gelStack.addGel(gb.buildGel());
		
		ResultSet output = gelStack.buildOutput(input);
		
		while (!exOut.isAfterLast() && !output.isAfterLast()) {
			assertTrue(checkRowEquality(exOut,input));
		}
		
		if (!exOut.isAfterLast() || !output.isAfterLast()) {
			assertTrue("Row count unequal", false);
		}
	}
	
	public static boolean checkRowEquality(ResultSet expected, ResultSet actual) {
		boolean result = false;
		
		return result;
	}
}
