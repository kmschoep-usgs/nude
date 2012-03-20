package gov.usgs.cida.nude.out;

import java.io.Closeable;
import java.sql.ResultSet;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Closers {
	private static final Logger log = LoggerFactory.getLogger(Closers.class);
	public static void closeQuietly(Closeable c) {
		IOUtils.closeQuietly(c);
	}
	
	public static void closeQuietly(ResultSet rs) {
		try {
			if (null != rs) {
				rs.close();
			}
		} catch (Exception e) {
			log.trace("Exception thrown while closing ResultSet", e);
		}
	}
}
