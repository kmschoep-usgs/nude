package examples.ida.test;

import static org.junit.Assert.assertEquals;
import examples.ida.IdaMetadataConnector;
import examples.ida.request.MetadataRequest;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.out.Closers;
import gov.usgs.cida.nude.out.Dispatcher;
import gov.usgs.cida.nude.out.StreamResponse;
import gov.usgs.cida.nude.out.TableResponse;
import gov.usgs.cida.nude.provider.http.HttpProvider;
import gov.usgs.cida.nude.resultset.inmemory.StringTableResultSet;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import gov.usgs.webservices.framework.basic.MimeType;

import java.io.StringWriter;
import java.sql.ResultSet;

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
	
//	@Ignore
	@Test
	public void testFormatsResponse() throws Exception {
		String expected = null;
		String actual = null;
		
		IdaMetadataConnector ida = new IdaMetadataConnector(httpProvider);
		
		StringTableResultSet params = new StringTableResultSet(new ColumnGrouping(MetadataRequest.sn));
		params.addRow(new TableRow(MetadataRequest.sn, "04085427"));
		
		ida.addInput(params);
		
		
		
		ResultSet rset = null;
		StringWriter sw = null;
		
		try {
			sw = new StringWriter();
			rset = ida.getResultSet();
			
			TableResponse resp = new TableResponse(rset);
			StreamResponse outStrm = Dispatcher.buildFormattedResponse(MimeType.XML, resp);
			StreamResponse.dispatch(outStrm, sw);
		} finally {
			Closers.closeQuietly(sw);
			Closers.closeQuietly(rset);
		}
		
		StringBuffer sbEx = new StringBuffer();
		sbEx.append("<?xml version='1.0' encoding='UTF-8'?>");
		sbEx.append("<success rowCount=\"-1\">");
		sbEx.append(	"<data>");
		sbEx.append(		"<MINDATETIME>");
		sbEx.append(			"1986-10-01 00:15:00.0");
		sbEx.append(		"</MINDATETIME>");
		sbEx.append(		"<MAXDATETIME>");
		sbEx.append(			"2007-09-30 23:45:00.0");
		sbEx.append(		"</MAXDATETIME>");
		sbEx.append(	"</data>");
		sbEx.append("</success>");
		
		expected = sbEx.toString();
		actual = sw.toString();
		
		assertEquals(expected, actual);
	}
	
}
