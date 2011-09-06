package examples.ida.test;


import static org.junit.Assert.assertEquals;
import examples.ida.IdaOverseer;
import gov.usgs.cida.nude.params.OutputFormat;
import gov.usgs.cida.nude.provider.http.HttpProvider;
import gov.usgs.cida.nude.resultset.StringTableResultSet;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;
import gov.usgs.cida.nude.values.TableRow;

import java.io.StringWriter;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class IdaOverseerTest {

	public static HttpProvider httpProvider = new HttpProvider();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		httpProvider.init();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		httpProvider.destroy();
	}
	
	protected String expected = null;
	protected String actual = null;
	
	protected StringWriter sw = null;
	protected StringBuffer sbEx = null;
	
	@Before
	public void setUp() throws Exception {
		sw = new StringWriter();
		sbEx = new StringBuffer();
	}

	@After
	public void tearDown() throws Exception {
		expected = null;
		actual = null;
		
		sw = null;
		sbEx = null;
	}
	
	@Test
	public void testSomethingNew() throws Exception {
		
		IdaOverseer overseer = new IdaOverseer();
		
		String siteNumber = "04085427";
		
		overseer.addInput(buildXmlFormatParam());
		overseer.addInput(buildIdaParams(siteNumber));
		
		overseer.dispatch(sw);
		
		sbEx.append("<?xml version='1.0' encoding='UTF-8'?>");
		sbEx.append("<success rowCount=\"1\">");
		sbEx.append(	"<data>");
		sbEx.append(		"<mindatetime>");
		sbEx.append(			"1986-10-01 00:15:00.0");
		sbEx.append(		"</mindatetime>");
		sbEx.append(		"<maxdatetime>");
		sbEx.append(			"2010-09-30 23:45:00.0");
		sbEx.append(		"</maxdatetime>");
		sbEx.append(	"</data>");
		sbEx.append("</success>");
		
		expected = sbEx.toString();
		actual = sw.toString();
		
		assertEquals(expected, actual);
	}
	
	public static ResultSet buildXmlFormatParam() {
		ResultSet result = null;
		
		ColumnGrouping colGroup = new ColumnGrouping(OutputFormat.FORMAT_TYPE, Arrays.asList(new Column[] {OutputFormat.FORMAT_TYPE, OutputFormat.SCHEMA_TYPE}));
		StringTableResultSet params = new StringTableResultSet(colGroup);
		Map<Column, String> row = new HashMap<Column, String>();
		row.put(OutputFormat.FORMAT_TYPE, "XML");
		row.put(OutputFormat.SCHEMA_TYPE, "camelCase");
		TableRow tableRow = new TableRow(colGroup, row);
		params.addRow(tableRow);
		result = params;
		
		return result;
	}
	
	public static ResultSet buildIdaParams(String siteNo) {
		ResultSet result = null;
		
		return result;
	}
}
