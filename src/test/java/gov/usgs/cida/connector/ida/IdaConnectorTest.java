package gov.usgs.cida.connector.ida;

import static org.junit.Assert.assertEquals;
import gov.usgs.cida.provider.http.HttpProvider;
import gov.usgs.cida.values.BuiltTable;
import gov.usgs.cida.values.TableRow;

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
		
	}
	
}
