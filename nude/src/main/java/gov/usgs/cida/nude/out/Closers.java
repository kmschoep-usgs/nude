package gov.usgs.cida.nude.out;

import gov.usgs.cida.nude.provider.sql.SQLProvider;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Closers {
	private static final Logger log = LoggerFactory.getLogger(Closers.class);
	public static void closeQuietly(Closeable c) {
		IOUtils.closeQuietly(c);
	}
	
//	public static void closeQuietly(ResultSet rs) {
//		try {
//			if (null != rs) {
//				rs.close();
//			}
//		} catch (Exception e) {
//			log.trace("Exception thrown while closing ResultSet", e);
//		}
//	}
	
	public static void closeQuietly(ResultSet rs) {
		try {
			if (null != rs) {
				Statement stmt = null;
				Connection con = null;
				try {
					stmt = rs.getStatement();
				} catch (SQLException ex) {
					log.trace("couldn't get statement", ex);
				}
				if (null != stmt) {
					try {
						con = stmt.getConnection();
					} catch (SQLException ex) {
						log.trace("couldn't get connection", ex);
					}
				}
				
				try {
					rs.close();
				} catch (Exception e) {
					log.trace("Exception thrown while closing ResultSet", e);
				}
				
				if (null != stmt) {
					try {
						stmt.close();
					} catch (Exception e) {
						log.trace("Exception thrown while closing ResultSet", e);
					}
				}
				
				if (null != con) {
					try {
						if (!con.isClosed()) {
							SQLProvider.closeConnection(con);
						} else {
							log.trace("Connection already closed!");
						}
					} catch (Exception e) {
						log.trace("Exception thrown while closing ResultSet", e);
					}
				}
			} else {
				log.trace("null ResultSet to close");
			}
		} catch (Exception e) {
			log.trace("Exception thrown while closing ResultSet", e);
		}
	}
}
