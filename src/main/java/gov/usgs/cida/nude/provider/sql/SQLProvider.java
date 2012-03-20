/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.usgs.cida.nude.provider.sql;

import gov.usgs.cida.nude.provider.IProvider;
import gov.usgs.cida.nude.resultset.inmemory.TypedValue;
import java.sql.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class SQLProvider implements IProvider {
	private static final Logger log = LoggerFactory.getLogger(SQLProvider.class);
	
	
	
	@Override
	public void init() {
		//TODO
	}
	
	public ResultSet getResults(ParameterizedString query) {
		ResultSet result = null;
		
		
		
		return result;
	}
	
	@Override
	public void destroy() {
		//TODO
	}
	
	
	private static AtomicInteger connectionCount = new AtomicInteger(0);
	/**
	 * This variable tells us how many cycles (getConnection called -> closeConnection called)
	 * are still in process.  Hopefully this will be a more accurate view than connectionCount.
	 */
	private static AtomicInteger unfinishedConCycles = new AtomicInteger(0);
	
	/**
	 * Factory method to create a Connection object to the Database
	 * @param dataSource
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws NamingException
	 */
	public static Connection getConnection(String dataSource) throws SQLException, NamingException, ClassNotFoundException {
		int connectionNumber = -1;
		Connection connection = null;
		try {
			if (null != dataSource) {
				connection = getJNDIConnection(dataSource);
			}
			if (null == connection) connection = getJDBCConnection();
		} finally {
			if (null != connection) {
				connectionNumber = connection.hashCode();
			}
			int conCycles = unfinishedConCycles.incrementAndGet();
			log.debug("Connection Cycle started:" + connectionNumber + ", Total Unfinished:" + conCycles);
		}
		return connection;
	}

	/**
	 * Attempts to attain a java.sql.Connection object via JDBC 
	 * 
	 * @return Open Connection object.  Null if not possible.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private static Connection getJDBCConnection() throws SQLException, ClassNotFoundException {
		Connection connection = null;
		log.trace("Could not find JNDI hook. Trying simple JDBC connection.");
		String dbuser 	= System.getProperty("dbuser");
		String dbpass 	= System.getProperty("dbpass");
		String url 		= System.getProperty("dburl");
		String clazz    = System.getProperty("dbclass");
        if (clazz == null) {
            clazz = "oracle.jdbc.OracleDriver";
        }

		Class.forName(clazz);
		connection = DriverManager.getConnection(url, dbuser, dbpass);
		int conCount = connectionCount.incrementAndGet();
		log.trace("Opened Testing Connection " + connection.hashCode()+ ". Total:" + conCount);
		return connection;

	}

	/**
	 * Attempts to attain a java.sql.Connection object via JNDI Naming 
	 * 
	 * @param dataSource JNDI string to connect to
	 * @return
	 */
	private static Connection getJNDIConnection(String dataSource) {
		Connection connection = null;

		Context ctx;
		try {
			ctx = new InitialContext();
			String dataSourceString = dataSource;
			DataSource ds = (DataSource) ctx.lookup(dataSourceString);
			connection = ds.getConnection();
			int conCount = connectionCount.incrementAndGet();
			log.trace("Opened Connection " + connection.hashCode()+ ". Total:" + conCount);
		} catch (NamingException e) {
			log.error("Could not open Connection object: " + e.getMessage());
			return null;
		} catch (SQLException e) {
			log.error("Could not open Connection object: " + e.getMessage());
			return null;
		}		
		return connection;
	}

	/**
	 * Closes a java.sql.Connection object 
	 * @param con java.sql.Connection that needs closing
	 */
	public static void closeConnection(Connection con) {
		int connectionNumber = -1;
		if (null != con) {
			try {
				connectionNumber = con.hashCode();
				if (con.isClosed()) {
					log.error("Connection " + connectionNumber + " is already closed!!");
				}
				log.trace("Closing Connection " + connectionNumber);
				con.close(); 
				int conCount = connectionCount.decrementAndGet();
				log.trace("Closed Connection. Total:" + conCount);
			} catch (Exception e) {
				log.error("Could not close Connection object: " + e.getMessage());
			} finally {
				int conCycles = unfinishedConCycles.decrementAndGet();
				log.debug("Connection Cycle finished:" + connectionNumber + ", Total Unfinished:" + conCycles);
			}
		} else {
			log.trace("closeConnection called on null object");
		}
	}

	/**
	 * Executes a database query, the criteria for which is provided by the query parameter
	 * 
	 * @param query The criteria for the query  
	 * @param con An open database connection object
	 * @return ResultSet object containing the results of the query 
	 * @throws SQLException
	 */
	public static ResultSet getQueryResults(String query, Connection con) throws SQLException {
		Statement statement = con.createStatement();
		ResultSet rset = statement.executeQuery(query);
		return rset;
	}
	
	/**
	 * Executes a database query via a PreparedStatement
	 * 
	 * @param query The ParameterizedString criteria for the query  
	 * @param con An open database connection object
	 * @return ResultSet object containing the results of the query 
	 * @throws SQLException
	 */
	public static ResultSet getQueryResults(ParameterizedString query, Connection con) throws SQLException {
		PreparedStatement statement = prepareStatement(query, con);
		return statement.executeQuery();
	}
	
	/**
	 * Executes a database update (INSERT, UPDATE, DELETE)
	 * on a string.
	 * 
	 * @param sql The String criteria for the update  
	 * @param con An open database connection object
	 * @return int the number of rows affected by the update 
	 * @throws SQLException
	 */
	public static int doUpdate(String sql, Connection con) throws SQLException {
		Statement statement = con.createStatement();
		int result = statement.executeUpdate(sql);
		return result;
	}
	
	/**
	 * Executes a database update (INSERT, UPDATE, DELETE)
	 * via a PreparedStatement
	 * 
	 * @param sql The ParameterizedString criteria for the query  
	 * @param con An open database connection object
	 * @return int the number of rows affected by the update 
	 * @throws SQLException
	 */
	public static int doUpdate(ParameterizedString sql, Connection con) throws SQLException {
		PreparedStatement statement = prepareStatement(sql, con);
		return statement.executeUpdate();
	}

	public static void setAutoCommit(Connection con, boolean b) {
		if (null != con) {
			try {
				log.trace("CONNECTION " + con.hashCode() +" AUTO_COMMIT: " + con.getAutoCommit() + " NEW: " + b);
				if (b != con.getAutoCommit()) {
					con.setAutoCommit(b);
				}
			} catch (SQLException e) {
				log.error("Caught Exception", e);
			}
		}
	}
	
	public static PreparedStatement prepareStatement(ParameterizedString sql, Connection con) throws SQLException {
		PreparedStatement result = con.prepareStatement(sql.getClause());
		
		List<TypedValue> values = sql.getValues();
		for (int i = 0; i < values.size(); i++) {
			result.setString(i+1, values.get(i).toString());
		}
		
		return result;
	}
}
