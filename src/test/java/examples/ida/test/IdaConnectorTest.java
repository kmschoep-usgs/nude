package examples.ida.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import examples.ida.IdaConnector;
import gov.usgs.cida.provider.http.HttpProvider;
import gov.usgs.cida.spec.formatting.ReturnType;
import gov.usgs.cida.spec.jsl.SpecResponse;
import gov.usgs.cida.spec.out.Closers;
import gov.usgs.cida.spec.out.Services;
import gov.usgs.cida.spec.out.StreamResponse;
import gov.usgs.cida.values.BuiltTable;
import gov.usgs.cida.values.TableRow;
import gov.usgs.webservices.framework.basic.FormatType;

import java.io.StringWriter;
import java.sql.ResultSet;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class IdaConnectorTest {
	
	public static HttpProvider httpProvider = new HttpProvider();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		httpProvider.init();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		httpProvider.destroy();
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetsResponse() throws Exception {
		IdaConnector ida = new IdaConnector(httpProvider);
		
		BuiltTable params = new BuiltTable();
		params.addRow(new TableRow("sn", "04085427"));
		
		ida.addInput(params.getResultSet());
		
		BuiltTable results = BuiltTable.consume(ida.getResultSet());
		
		if (0 < results.getRowCount()) {
			String expected;
			String actual;
			for (TableRow row : results) {
				expected = "1986-10-01 00:15:00.0";
				actual = row.getValue("mindatetime");
				assertEquals(expected, actual);
				
				expected = "2010-09-30 23:45:00.0";
				actual = row.getValue("maxdatetime");
				assertEquals(expected, actual);
			}
		} else {
			assertTrue(false);
		}
	}
	
	@Test
	public void testFormatsResponse() throws Exception {
		String expected = null;
		String actual = null;
		
		IdaConnector ida = new IdaConnector(httpProvider);
		
		BuiltTable params = new BuiltTable();
		params.addRow(new TableRow("sn", "04085427"));
		
		ida.addInput(params.getResultSet());
		
		ResultSet rset = null;
		StringWriter sw = null;
		
		try {
			sw = new StringWriter();
			rset = ida.getResultSet();
			
			SpecResponse resp = new SpecResponse(ida.getSpec(), rset, ida.getRowCount());
			StreamResponse outStrm = Services.buildFormattedResponse(ReturnType.xml, FormatType.XML, resp);
			StreamResponse.dispatch(outStrm, sw);
		} finally {
			Closers.closeQuietly(sw);
			Closers.closeQuietly(rset);
		}
		
		StringBuffer sbEx = new StringBuffer();
		sbEx.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
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
	
}
