package gov.usgs.cida.nude.resultset.inmemory;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.column.ColumnGrouping;
import gov.usgs.cida.nude.column.SimpleColumn;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 *
 * @author jiwalker
 */
public class TableRowTest {
	
	private static final Column testCol = new SimpleColumn("test");
	private static final ColumnGrouping testCG = new ColumnGrouping(testCol);
	private static final Map<Column, String> testMap = new HashMap<Column, String>();
	
	@BeforeClass
	public static void setUpAll() {
		testMap.put(testCol, "testVal");
	}
	
	/**
	 * Test of getValue method, of class TableRow.
	 */
	@Test
	public void testGetValue() {
		TableRow instance = new TableRow(testCol, testMap.get(testCol));
		String expResult = "testVal";
		String result = instance.getValue(testCol);
		assertEquals(expResult, result);
	}

	/**
	 * Test of getColumns method, of class TableRow.
	 */
	@Test
	public void testGetColumns() {
		TableRow instance = new TableRow(testCG, testMap);
		ColumnGrouping result = instance.getColumns();
		assertEquals(testCG, result);
	}

	@Test
	public void testSerialization() throws IOException, ClassNotFoundException {
		TableRow instance = new TableRow(testCG, testMap);
		File tmpFile = File.createTempFile("test", "tmp");
		FileOutputStream fout = null;
		ObjectOutput objOut = null;
		try {
			fout = new FileOutputStream(tmpFile);
			objOut = new ObjectOutputStream(fout);
			objOut.writeObject(instance);
		} finally {
			objOut.flush();
			IOUtils.closeQuietly(fout);
		}
		
		FileInputStream fin = null;
		ObjectInput objIn = null;
		TableRow result = null;
		try {
			fin = new FileInputStream(tmpFile);
			objIn = new ObjectInputStream(fin);
			result = (TableRow) objIn.readObject();
		} finally {
			IOUtils.closeQuietly(fin);
			FileUtils.deleteQuietly(tmpFile);
		}
		assertThat(result, is(equalTo(instance)));
	}
	
}
