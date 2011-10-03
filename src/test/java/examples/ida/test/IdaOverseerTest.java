package examples.ida.test;


import static org.junit.Assert.assertEquals;
import examples.ida.IdaOverseer;
import examples.ida.request.IdaConnectorParams;
import gov.usgs.cida.nude.params.OutputFormat;
import gov.usgs.cida.nude.provider.http.HttpProvider;
import gov.usgs.cida.nude.resultset.ColumnGroupedResultSet;
import gov.usgs.cida.nude.resultset.StringTableResultSet;
import gov.usgs.cida.nude.table.Column;
import gov.usgs.cida.nude.table.ColumnGrouping;
import gov.usgs.cida.nude.table.DummyColumn;
import gov.usgs.cida.nude.values.TableRow;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
	
	@Ignore
	@Test
	public void testSomethingNew() throws Exception {
		
		IdaOverseer overseer = new IdaOverseer(httpProvider);
		
		String siteNumber = "04085427";
		
		overseer.addInput(buildXmlFormatParam());
		overseer.addInput(buildIdaParams(siteNumber));
		
		overseer.dispatch(sw);
		
		expected = getExpectedResponse();
		actual = sw.toString();
		
		assertEquals(expected, actual);
	}
	
	public static ColumnGroupedResultSet buildXmlFormatParam() {
		ColumnGroupedResultSet result = null;
		
		ColumnGrouping colGroup = new ColumnGrouping(DummyColumn.DUMMY, Arrays.asList(new Column[] {DummyColumn.DUMMY, OutputFormat.FORMAT_TYPE, OutputFormat.SCHEMA_TYPE}));
		StringTableResultSet params = new StringTableResultSet(colGroup);
		Map<Column, String> row = new HashMap<Column, String>();
		row.put(DummyColumn.DUMMY, "1");
		row.put(OutputFormat.FORMAT_TYPE, "XML");
		row.put(OutputFormat.SCHEMA_TYPE, "passthrough");
		TableRow tableRow = new TableRow(colGroup, row);
		params.addRow(tableRow);
		result = params;
		
		return result;
	}
	
	public static ColumnGroupedResultSet buildIdaParams(String siteNo) {
		ColumnGroupedResultSet result = null;
		
		List<Column> colList = new ArrayList<Column>();
		colList.add(DummyColumn.DUMMY);
		colList.addAll(Arrays.asList(IdaConnectorParams.values()));
		ColumnGrouping colGroup = new ColumnGrouping(DummyColumn.DUMMY, colList);
		
		StringTableResultSet params = new StringTableResultSet(colGroup);
		
		Map<Column, String> row = new HashMap<Column, String>();
		row.put(DummyColumn.DUMMY, "1");
		row.put(IdaConnectorParams.GET_DATA, "true");
		row.put(IdaConnectorParams.SITE_NUMBER, siteNo);
		row.put(IdaConnectorParams.TO_DATE, "2010-08-30");
		row.put(IdaConnectorParams.FROM_DATE, "2010-08-30");
		
		TableRow tableRow = new TableRow(colGroup, row);
		params.addRow(tableRow);
		result = params;
		
		return result;
	}
	
	public static String getExpectedResponse() {
		StringBuilder result = new StringBuilder();
		
		result.append("<?xml version='1.0' encoding='UTF-8'?>");
		result.append("<success rowCount=\"-1\">");
		for (int i = 0; i < dates.length && i < values.length; i++) {
			result.append(	"<data>");
			result.append(		"<timestamp>");
			result.append(			dates[i]);
			result.append(		"</timestamp>");
			result.append(		"<value>");
			result.append(			values[i]);
			result.append(		"</value>");
			result.append(	"</data>");
		}
		
		result.append("</success>");
		
		return result.toString();
	}
	
	protected static String[] dates = new String[] {
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
	
	protected static String[] values = new String[] {
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
