package gov.usgs.cida.nude.column;

import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 * @author jiwalker
 */
public class CGResultSetMetaDataTest {
	
	@Test
	public void testSerialization() throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		List<Column> cols = new LinkedList<Column>();
		for (int i=1; i<10; i++) {
			cols.add(new SimpleColumn("test" + i));
		}
		ColumnGrouping testCG = new ColumnGrouping(cols);
		
		CGResultSetMetaData cgResultSetMetaData = new CGResultSetMetaData(testCG);
		File tmpFile = File.createTempFile("test", "tmp");
		FileOutputStream fout = null;
		ObjectOutput objOut = null;
		try {
			fout = new FileOutputStream(tmpFile);
			objOut = new ObjectOutputStream(fout);
			objOut.writeObject(cgResultSetMetaData);
		} finally {
			objOut.flush();
			IOUtils.closeQuietly(fout);
		}
		
		FileInputStream fin = null;
		ObjectInput objIn = null;
		CGResultSetMetaData result = null;
		try {
			fin = new FileInputStream(tmpFile);
			objIn = new ObjectInputStream(fin);
			result = (CGResultSetMetaData) objIn.readObject();
		} finally {
			IOUtils.closeQuietly(fin);
			FileUtils.deleteQuietly(tmpFile);
		}
		assertThat(result.getColumnCount(), is(equalTo(cgResultSetMetaData.getColumnCount())));
		assertThat(result.getColumnName(1), is(equalTo(cgResultSetMetaData.getColumnName(1))));
		assertThat(result.getColumnName(9), is(equalTo(cgResultSetMetaData.getColumnName(9))));
	}

	
}
