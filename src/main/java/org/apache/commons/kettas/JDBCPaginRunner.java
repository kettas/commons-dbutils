package org.apache.commons.kettas;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.Pagin;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.RowProcessor;

/**
 * JDBC分页扩展 可支持的数据库类型包括MSsql,MySql,Oracle
 * 
 * @author 杨伦亮 上午1:13:40
 */
public class JDBCPaginRunner {
	private static Long count = 0l;
	private static SQLVersion.VersionName dataBaseVersion = null;
	/**
	 * Is {@link ParameterMetaData#getParameterType(int)} broken (have we tried
	 * it yet)?
	 */
	private volatile boolean pmdKnownBroken = true;

	/**
	 * The DataSource to retrieve connections from.
	 */
	protected final DataSource ds;

	/**
	 * Constructor for JDBCPaginRunner.
	 */
	public JDBCPaginRunner() {
		super();
		ds = null;
	}

	/**
	 * Constructor for JDBCPaginRunner, allows workaround for Oracle drivers
	 * 
	 * @param pmdKnownBroken
	 *            Oracle drivers don't support
	 *            {@link ParameterMetaData#getParameterType(int) }; if
	 *            <code>pmdKnownBroken</code> is set to true, we won't even
	 *            try it; if false, we'll try it, and if it breaks, we'll
	 *            remember not to use it again.
	 */
	public JDBCPaginRunner(boolean pmdKnownBroken) {
		super();
		this.pmdKnownBroken = pmdKnownBroken;
		ds = null;
	}

	/**
	 * Constructor for JDBCPaginRunner, allows workaround for Oracle drivers.
	 * Methods that do not take a <code>Connection</code> parameter will
	 * retrieve connections from this <code>DataSource</code>.
	 * 
	 * @param ds
	 *            The <code>DataSource</code> to retrieve connections from.
	 */
	public JDBCPaginRunner(DataSource ds) {
		super();
		this.ds = ds;
	}

	/**
	 * Constructor for JDBCPaginRunner, allows workaround for Oracle drivers.
	 * Methods that do not take a <code>Connection</code> parameter will
	 * retrieve connections from this <code>DataSource</code>.
	 * 
	 * @param ds
	 *            The <code>DataSource</code> to retrieve connections from.
	 * @param pmdKnownBroken
	 *            Oracle drivers don't support
	 *            {@link ParameterMetaData#getParameterType(int) }; if
	 *            <code>pmdKnownBroken</code> is set to true, we won't even
	 *            try it; if false, we'll try it, and if it breaks, we'll
	 *            remember not to use it again.
	 */
	public JDBCPaginRunner(DataSource ds, boolean pmdKnownBroken) {
		super();
		this.pmdKnownBroken = pmdKnownBroken;
		this.ds = ds;
	}

	/**
	 * Execute a batch of SQL INSERT, UPDATE, or DELETE queries.
	 * 
	 * @param conn
	 *            The Connection to use to run the query. The caller is
	 *            responsible for closing this Connection.
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            An array of query replacement parameters. Each row in this
	 *            array is one set of batch replacement values.
	 * @return The number of rows updated per statement.
	 * @throws SQLException
	 *             if a database access error occurs
	 * @since DbUtils 1.1
	 */
	public int[] batch(Connection conn, String sql, Object[][] params)
			throws SQLException {

		PreparedStatement stmt = null;
		int[] rows = null;
		try {
			stmt = this.prepareStatement(conn, sql);

			for (int i = 0; i < params.length; i++) {
				this.fillStatement(stmt, params[i]);
				stmt.addBatch();
			}
			rows = stmt.executeBatch();

		} catch (SQLException e) {
			this.rethrow(e, sql, (Object[]) params);
		} finally {
			close(stmt);
		}

		return rows;
	}

	/**
	 * Execute a batch of SQL INSERT, UPDATE, or DELETE queries. The
	 * <code>Connection</code> is retrieved from the <code>DataSource</code>
	 * set in the constructor. This <code>Connection</code> must be in
	 * auto-commit mode or the update will not be saved.
	 * 
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            An array of query replacement parameters. Each row in this
	 *            array is one set of batch replacement values.
	 * @return The number of rows updated per statement.
	 * @throws SQLException
	 *             if a database access error occurs
	 * @since DbUtils 1.1
	 */
	public int[] batch(String sql, Object[][] params) throws SQLException {
		Connection conn = this.prepareConnection();

		try {
			return this.batch(conn, sql, params);
		} finally {
			close(conn);
		}
	}

	/**
	 * Fill the <code>PreparedStatement</code> replacement parameters with the
	 * given objects.
	 * 
	 * @param stmt
	 *            PreparedStatement to fill
	 * @param params
	 *            Query replacement parameters; <code>null</code> is a valid
	 *            value to pass in.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void fillStatement(PreparedStatement stmt, Object... params)
			throws SQLException {

		if (params == null) {
			return;
		}
		ParameterMetaData pmd = null;
		if (!pmdKnownBroken) {
			pmd = stmt.getParameterMetaData();
			if (pmd.getParameterCount() < params.length) {
				throw new SQLException("Too many parameters: expected "
						+ pmd.getParameterCount() + ", was given "
						+ params.length);
			}
		}
		for (int i = 0; i < params.length; i++) {
			if (params[i] != null) {
				if (params[i] instanceof java.lang.String
						&& params[i].toString().length() < 1) {
					int sqlType = Types.VARCHAR;
					if (!pmdKnownBroken) {
						try {
							sqlType = pmd.getParameterType(i + 1);
						} catch (SQLException e) {
							pmdKnownBroken = true;
						}
					}
					stmt.setNull(i + 1, sqlType);
				} else {
					stmt.setObject(i + 1, params[i]);
				}
			} else {
				// VARCHAR works with many drivers regardless
				// of the actual column type. Oddly, NULL and
				// OTHER don't work with Oracle's drivers.
				int sqlType = Types.VARCHAR;
				if (!pmdKnownBroken) {
					try {
						sqlType = pmd.getParameterType(i + 1);
					} catch (SQLException e) {
						pmdKnownBroken = true;
					}
				}
				stmt.setNull(i + 1, sqlType);
			}
		}
	}

	/**
	 * Fill the <code>PreparedStatement</code> replacement parameters with the
	 * given object's bean property values.
	 * 
	 * @param stmt
	 *            PreparedStatement to fill
	 * @param bean
	 *            a JavaBean object
	 * @param properties
	 *            an ordered array of properties; this gives the order to insert
	 *            values in the statement
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void fillStatementWithBean(PreparedStatement stmt, Object bean,
			PropertyDescriptor[] properties) throws SQLException {
		Object[] params = new Object[properties.length];
		for (int i = 0; i < properties.length; i++) {
			PropertyDescriptor property = properties[i];
			Object value = null;
			Method method = property.getReadMethod();
			if (method == null) {
				throw new RuntimeException("No read method for bean property "
						+ bean.getClass() + " " + property.getName());
			}
			try {
				value = method.invoke(bean, new Object[0]);
			} catch (InvocationTargetException e) {
				throw new RuntimeException("Couldn't invoke method: " + method,
						e);
			} catch (IllegalArgumentException e) {
				throw new RuntimeException(
						"Couldn't invoke method with 0 arguments: " + method, e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Couldn't invoke method: " + method,
						e);
			}
			params[i] = value;
		}
		fillStatement(stmt, params);
	}

	/**
	 * Fill the <code>PreparedStatement</code> replacement parameters with the
	 * given object's bean property values.
	 * 
	 * @param stmt
	 *            PreparedStatement to fill
	 * @param bean
	 *            a JavaBean object
	 * @param propertyNames
	 *            an ordered array of property names (these should match the
	 *            getters/setters); this gives the order to insert values in the
	 *            statement
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void fillStatementWithBean(PreparedStatement stmt, Object bean,
			String... propertyNames) throws SQLException {
		PropertyDescriptor[] descriptors;
		try {
			descriptors = Introspector.getBeanInfo(bean.getClass())
					.getPropertyDescriptors();
		} catch (IntrospectionException e) {
			throw new RuntimeException("Couldn't introspect bean "
					+ bean.getClass().toString(), e);
		}
		PropertyDescriptor[] sorted = new PropertyDescriptor[propertyNames.length];
		for (int i = 0; i < propertyNames.length; i++) {
			String propertyName = propertyNames[i];
			if (propertyName == null) {
				throw new NullPointerException("propertyName can't be null: "
						+ i);
			}
			boolean found = false;
			for (int j = 0; j < descriptors.length; j++) {
				PropertyDescriptor descriptor = descriptors[j];
				if (propertyName.equals(descriptor.getName())) {
					sorted[i] = descriptor;
					found = true;
					break;
				}
			}
			if (!found) {
				throw new RuntimeException("Couldn't find bean property: "
						+ bean.getClass() + " " + propertyName);
			}
		}
		fillStatementWithBean(stmt, bean, sorted);
	}

	/**
	 * Returns the <code>DataSource</code> this runner is using.
	 * <code>JDBCPaginRunner</code> methods always call this method to get the
	 * <code>DataSource</code> so subclasses can provide specialized behavior.
	 * 
	 * @return DataSource the runner is using
	 */
	public DataSource getDataSource() {
		return this.ds;
	}

	/**
	 * Factory method that creates and initializes a
	 * <code>PreparedStatement</code> object for the given SQL.
	 * <code>JDBCPaginRunner</code> methods always call this method to prepare
	 * statements for them. Subclasses can override this method to provide
	 * special PreparedStatement configuration if needed. This implementation
	 * simply calls <code>conn.prepareStatement(sql)</code>.
	 * 
	 * @param conn
	 *            The <code>Connection</code> used to create the
	 *            <code>PreparedStatement</code>
	 * @param sql
	 *            The SQL statement to prepare.
	 * @return An initialized <code>PreparedStatement</code>.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	protected PreparedStatement prepareStatement(Connection conn, String sql)
			throws SQLException {
		return conn.prepareStatement(sql);
	}
	
	/**
     * 提供优化查询接口-自定义查询方式
     */
	protected PreparedStatement prepareStatement(Connection conn, String sql,int resultSetType, int resultSetConcurrency)
	throws SQLException {
		return conn.prepareStatement(sql,resultSetType,resultSetConcurrency);
	}
	/**
	 * Factory method that creates and initializes a <code>Connection</code>
	 * object. <code>JDBCPaginRunner</code> methods always call this method to
	 * retrieve connections from its DataSource. Subclasses can override this
	 * method to provide special <code>Connection</code> configuration if
	 * needed. This implementation simply calls <code>ds.getConnection()</code>.
	 * 
	 * @return An initialized <code>Connection</code>.
	 * @throws SQLException
	 *             if a database access error occurs
	 * @since DbUtils 1.1
	 */
	protected Connection prepareConnection() throws SQLException {
		if (this.getDataSource() == null) {
			throw new SQLException(
					"JDBCPaginRunner requires a DataSource to be "
							+ "invoked in this way, or a Connection should be passed in");
		}
		return this.getDataSource().getConnection();
	}

	/**
	 * Execute an SQL SELECT query with a single replacement parameter. The
	 * caller is responsible for closing the connection.
	 * 
	 * @param <T>
	 *            The type of object that the handler returns
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param param
	 *            The replacement parameter.
	 * @param rsh
	 *            The handler that converts the results into an object.
	 * @return The object returned by the handler.
	 * @throws SQLException
	 *             if a database access error occurs
	 * @deprecated Use
	 *             {@link #query(Connection, String, ResultSetHandler, Object...)}
	 */
	public <T> T query(Connection conn, String sql, Object param,
			ResultSetHandler<T> rsh) throws SQLException {

		return this.<T> query(conn, sql, rsh, new Object[] { param });
	}

	/**
	 * Execute an SQL SELECT query with replacement parameters. The caller is
	 * responsible for closing the connection.
	 * 
	 * @param <T>
	 *            The type of object that the handler returns
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param params
	 *            The replacement parameters.
	 * @param rsh
	 *            The handler that converts the results into an object.
	 * @return The object returned by the handler.
	 * @throws SQLException
	 *             if a database access error occurs
	 * @deprecated Use
	 *             {@link #query(Connection,String,ResultSetHandler,Object...)}
	 *             instead
	 */
	public <T> T query(Connection conn, String sql, Object[] params,
			ResultSetHandler<T> rsh) throws SQLException {
		return query(conn, sql, rsh, params);
	}

	/**
	 * Execute an SQL SELECT query with replacement parameters. The caller is
	 * responsible for closing the connection.
	 * 
	 * @param <T>
	 *            The type of object that the handler returns
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param rsh
	 *            The handler that converts the results into an object.
	 * @param params
	 *            The replacement parameters.
	 * @return The object returned by the handler.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public <T> T query(Connection conn, String sql, ResultSetHandler<T> rsh,
			Object... params) throws SQLException {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		T result = null;

		try {
			if (dataBaseVersion == null) {
				dataBaseVersion = SQLVersion.getVersionName(conn);
			}
			if(dataBaseVersion != SQLVersion.VersionName.Sqlite){
				stmt = this.prepareStatement(conn,sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			}else{
				stmt=this.prepareStatement(conn, sql);
			}
			this.fillStatement(stmt, params);
			rs = this.wrap(stmt.executeQuery());
			result = rsh.handle(rs);

		} catch (SQLException e) {
			this.rethrow(e, sql, params);

		} finally {
			try {
				close(rs);
			} finally {
				close(stmt);
			}
		}

		return result;
	}

	/**
	 * Execute an SQL SELECT query without any replacement parameters. The
	 * caller is responsible for closing the connection.
	 * 
	 * @param <T>
	 *            The type of object that the handler returns
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param rsh
	 *            The handler that converts the results into an object.
	 * @return The object returned by the handler.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public <T> T query(Connection conn, String sql, ResultSetHandler<T> rsh)
			throws SQLException {

		return this.query(conn, sql, rsh, (Object[]) null);
	}

	/**
	 * Executes the given SELECT SQL with a single replacement parameter. The
	 * <code>Connection</code> is retrieved from the <code>DataSource</code>
	 * set in the constructor.
	 * 
	 * @param <T>
	 *            The type of object that the handler returns
	 * @param sql
	 *            The SQL statement to execute.
	 * @param param
	 *            The replacement parameter.
	 * @param rsh
	 *            The handler used to create the result object from the
	 *            <code>ResultSet</code>.
	 * 
	 * @return An object generated by the handler.
	 * @throws SQLException
	 *             if a database access error occurs
	 * @deprecated Use {@link #query(String, ResultSetHandler, Object...)}
	 */
	public <T> T query(String sql, Object param, ResultSetHandler<T> rsh)
			throws SQLException {

		return this.query(sql, rsh, new Object[] { param });
	}

	/**
	 * Executes the given SELECT SQL query and returns a result object. The
	 * <code>Connection</code> is retrieved from the <code>DataSource</code>
	 * set in the constructor.
	 * 
	 * @param <T>
	 *            The type of object that the handler returns
	 * @param sql
	 *            The SQL statement to execute.
	 * @param params
	 *            Initialize the PreparedStatement's IN parameters with this
	 *            array.
	 * 
	 * @param rsh
	 *            The handler used to create the result object from the
	 *            <code>ResultSet</code>.
	 * 
	 * @return An object generated by the handler.
	 * @throws SQLException
	 *             if a database access error occurs
	 * @deprecated Use {@link #query(String, ResultSetHandler, Object...)}
	 */
	public <T> T query(String sql, Object[] params, ResultSetHandler<T> rsh)
			throws SQLException {
		return query(sql, rsh, params);
	}

	/**
	 * Executes the given SELECT SQL query and returns a result object. The
	 * <code>Connection</code> is retrieved from the <code>DataSource</code>
	 * set in the constructor.
	 * 
	 * @param <T>
	 *            The type of object that the handler returns
	 * @param sql
	 *            The SQL statement to execute.
	 * @param rsh
	 *            The handler used to create the result object from the
	 *            <code>ResultSet</code>.
	 * @param params
	 *            Initialize the PreparedStatement's IN parameters with this
	 *            array.
	 * @return An object generated by the handler.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public <T> T query(String sql, ResultSetHandler<T> rsh, Object... params)
			throws SQLException {

		Connection conn = this.prepareConnection();

		try {
			return this.query(conn, sql, rsh, params);
		} finally {
			close(conn);
		}
	}

	/**
	 * Executes the given SELECT SQL without any replacement parameters. The
	 * <code>Connection</code> is retrieved from the <code>DataSource</code>
	 * set in the constructor.
	 * 
	 * @param <T>
	 *            The type of object that the handler returns
	 * @param sql
	 *            The SQL statement to execute.
	 * @param rsh
	 *            The handler used to create the result object from the
	 *            <code>ResultSet</code>.
	 * 
	 * @return An object generated by the handler.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public <T> T query(String sql, ResultSetHandler<T> rsh) throws SQLException {
		return this.query(sql, rsh, (Object[]) null);
	}

	/**
	 * Throws a new exception with a more informative error message.
	 * 
	 * @param cause
	 *            The original exception that will be chained to the new
	 *            exception when it's rethrown.
	 * 
	 * @param sql
	 *            The query that was executing when the exception happened.
	 * 
	 * @param params
	 *            The query replacement parameters; <code>null</code> is a
	 *            valid value to pass in.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	protected void rethrow(SQLException cause, String sql, Object... params)
			throws SQLException {
		String causeMessage = cause.getMessage();
		if (causeMessage == null) {
			causeMessage = "";
		}
		StringBuilder msg = new StringBuilder(causeMessage);

		msg.append(" Query: ");
		msg.append(sql);

		if (params != null) {
			msg.append(" Parameters: ");
			msg.append(Arrays.deepToString(params));
		}

		SQLException e = new SQLException(msg.toString(), cause.getSQLState(),
				cause.getErrorCode());
		e.setNextException(cause);

		throw e;
	}

	/**
	 * Execute an SQL INSERT, UPDATE, or DELETE query without replacement
	 * parameters.
	 * 
	 * @param conn
	 *            The connection to use to run the query.
	 * @param sql
	 *            The SQL to execute.
	 * @return The number of rows updated.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public int update(Connection conn, String sql) throws SQLException {
		return this.update(conn, sql, (Object[]) null);
	}

	/**
	 * Execute an SQL INSERT, UPDATE, or DELETE query with a single replacement
	 * parameter.
	 * 
	 * @param conn
	 *            The connection to use to run the query.
	 * @param sql
	 *            The SQL to execute.
	 * @param param
	 *            The replacement parameter.
	 * @return The number of rows updated.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public int update(Connection conn, String sql, Object param)
			throws SQLException {

		return this.update(conn, sql, new Object[] { param });
	}

	/**
	 * Execute an SQL INSERT, UPDATE, or DELETE query.
	 * 
	 * @param conn
	 *            The connection to use to run the query.
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            The query replacement parameters.
	 * @return The number of rows updated.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public int update(Connection conn, String sql, Object... params)
			throws SQLException {

		PreparedStatement stmt = null;
		int rows = 0;

		try {
			stmt = this.prepareStatement(conn, sql);
			this.fillStatement(stmt, params);
			rows = stmt.executeUpdate();

		} catch (SQLException e) {
			this.rethrow(e, sql, params);

		} finally {
			close(stmt);
		}

		return rows;
	}

	/**
	 * Executes the given INSERT, UPDATE, or DELETE SQL statement without any
	 * replacement parameters. The <code>Connection</code> is retrieved from
	 * the <code>DataSource</code> set in the constructor. This
	 * <code>Connection</code> must be in auto-commit mode or the update will
	 * not be saved.
	 * 
	 * @param sql
	 *            The SQL statement to execute.
	 * @throws SQLException
	 *             if a database access error occurs
	 * @return The number of rows updated.
	 */
	public int update(String sql) throws SQLException {
		return this.update(sql, (Object[]) null);
	}

	/**
	 * Executes the given INSERT, UPDATE, or DELETE SQL statement with a single
	 * replacement parameter. The <code>Connection</code> is retrieved from
	 * the <code>DataSource</code> set in the constructor. This
	 * <code>Connection</code> must be in auto-commit mode or the update will
	 * not be saved.
	 * 
	 * @param sql
	 *            The SQL statement to execute.
	 * @param param
	 *            The replacement parameter.
	 * @throws SQLException
	 *             if a database access error occurs
	 * @return The number of rows updated.
	 */
	public int update(String sql, Object param) throws SQLException {
		return this.update(sql, new Object[] { param });
	}

	/**
	 * Executes the given INSERT, UPDATE, or DELETE SQL statement. The
	 * <code>Connection</code> is retrieved from the <code>DataSource</code>
	 * set in the constructor. This <code>Connection</code> must be in
	 * auto-commit mode or the update will not be saved.
	 * 
	 * @param sql
	 *            The SQL statement to execute.
	 * @param params
	 *            Initializes the PreparedStatement's IN (i.e. '?') parameters.
	 * @throws SQLException
	 *             if a database access error occurs
	 * @return The number of rows updated.
	 */
	public int update(String sql, Object... params) throws SQLException {
		Connection conn = this.prepareConnection();

		try {
			return this.update(conn, sql, params);
		} finally {
			close(conn);
		}
	}

	/**
	 * Wrap the <code>ResultSet</code> in a decorator before processing it.
	 * This implementation returns the <code>ResultSet</code> it is given
	 * without any decoration.
	 * 
	 * <p>
	 * Often, the implementation of this method can be done in an anonymous
	 * inner class like this:
	 * </p>
	 * 
	 * <pre>
	 * JDBCPaginRunner run = new JDBCPaginRunner() {
	 * 	protected ResultSet wrap(ResultSet rs) {
	 * 		return StringTrimmedResultSet.wrap(rs);
	 * 	}
	 * };
	 * </pre>
	 * 
	 * @param rs
	 *            The <code>ResultSet</code> to decorate; never
	 *            <code>null</code>.
	 * @return The <code>ResultSet</code> wrapped in some decorator.
	 */
	protected ResultSet wrap(ResultSet rs) {
		return rs;
	}

	/**
	 * Close a <code>Connection</code>. This implementation avoids closing if
	 * null and does <strong>not</strong> suppress any exceptions. Subclasses
	 * can override to provide special handling like logging.
	 * 
	 * @param conn
	 *            Connection to close
	 * @throws SQLException
	 *             if a database access error occurs
	 * @since DbUtils 1.1
	 */
	protected void close(Connection conn) throws SQLException {
		DbUtils.close(conn);
	}

	/**
	 * Close a <code>Statement</code>. This implementation avoids closing if
	 * null and does <strong>not</strong> suppress any exceptions. Subclasses
	 * can override to provide special handling like logging.
	 * 
	 * @param stmt
	 *            Statement to close
	 * @throws SQLException
	 *             if a database access error occurs
	 * @since DbUtils 1.1
	 */
	protected void close(Statement stmt) throws SQLException {
		DbUtils.close(stmt);
	}

	/**
	 * Close a <code>ResultSet</code>. This implementation avoids closing if
	 * null and does <strong>not</strong> suppress any exceptions. Subclasses
	 * can override to provide special handling like logging.
	 * 
	 * @param rs
	 *            ResultSet to close
	 * @throws SQLException
	 *             if a database access error occurs
	 * @since DbUtils 1.1
	 */
	protected void close(ResultSet rs) throws SQLException {
		DbUtils.close(rs);
	}

	/**
	 * Oracle 分页
	 * 
	 * @param <T>
	 * @param conn
	 * @param sql
	 * @param params
	 * @param rsh
	 * @param start
	 * @param end
	 * @return
	 * @throws SQLException
	 */
	public <T> T limitOracle(Connection conn, String sql, Object[] params,
			ResultSetHandler<T> rsh, int start, int end) throws SQLException {
		T result = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			sql = "select * from (select row_.*,rownum rownum_  from ( " + sql
					+ ")row_ where  rownum <= " + end + ")  where   rownum_ > "
					+ start;
			ps = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			if (params != null && params.length > 0) {
				this.fillStatement(ps, params);
			}
			rs = ps.executeQuery();
			if (rs.next()) {
				rs.beforeFirst();
				return rsh.handle(rs);
			}
		} finally {
			if (rs != null) {
				DbUtils.closeQuietly(rs);
			}
			if (ps != null) {
				DbUtils.closeQuietly(ps);
			}
		}
		return result;
	}
	public <T> T limitSqlLit(Connection conn, String sql, Object[] params,
			ResultSetHandler<T> rsh, int start, int end) throws SQLException {
		T result = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			sql = sql + " limit " + (start > 0 ? (start + "," + end) : end);
			ps = conn.prepareStatement(sql);
			if (params != null && params.length > 0) {
				this.fillStatement(ps, params);
			}
			rs = ps.executeQuery();
			return rsh.handle(rs);
		} finally {
			if (rs != null) {
				DbUtils.closeQuietly(rs);
			}
			if (ps != null) {
				DbUtils.closeQuietly(ps);
			}
		}
	}
	/**
	 * Mysql方式分页处理
	 * 
	 * @param <T>
	 * @param conn
	 * @param sql
	 * @param params
	 * @param rsh
	 * @param start
	 * @param end
	 * @return
	 * @throws SQLException
	 */
	public <T> T limitMysql(Connection conn, String sql, Object[] params,
			ResultSetHandler<T> rsh, int start, int end) throws SQLException {
		T result = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			sql = sql + " limit " + (start > 0 ? (start + "," + end) : end);
			ps = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,java.sql.ResultSet.CONCUR_READ_ONLY);
			if (params != null && params.length > 0) {
				this.fillStatement(ps, params);
			}
			rs = ps.executeQuery();
			if (rs.next()) {
				rs.beforeFirst();
				return rsh.handle(rs);
			}
		} finally {
			if (rs != null) {
				DbUtils.closeQuietly(rs);
			}
			if (ps != null) {
				DbUtils.closeQuietly(ps);
			}
		}
		return result;
	}

	/**
	 * Mysql方式分页处理
	 * 
	 * @param <T>
	 * @param conn
	 * @param sql
	 * @param params
	 * @param rsh
	 * @param start
	 * @param end
	 * @return
	 * @throws SQLException
	 */
	public <T> T limitMSsql(Connection conn, String sql, Object[] params,
			ResultSetHandler<T> rsh, int start, int end) throws SQLException {
		/*
		 * 
		 * 2)、MSSQL: @sql limit @start,@end
		 */
		T result = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			sql = sql.replaceFirst("select", "select top " + end + " ");
			ps = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			if (params != null && params.length > 0) {
				this.fillStatement(ps, params);
			}
			ps.setMaxRows(end);
			rs = ps.executeQuery();
			if (rs.next()) {
				rs.absolute(start);
				result = rsh.handle(rs);
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			// if(conn!=null){
			// conn.close();
			// }
		}
		return result;
	}

	/**
	 * 指针分页
	 * 
	 * @param <T>
	 * @param conn
	 *            数据库的连接
	 * @param sql
	 *            执行的sql
	 * @param params
	 *            执行sql补充的参数
	 * @param rsh
	 *            数据处理方法(接口)
	 * @param start
	 *            指针开始的位置
	 * @param count
	 *            指针返回的最大记录数量(用此便可实现sqlserver 分页)
	 * @throws Exception
	 */
	public <T> T limit(Connection conn, String sql, Object[] params,
			ResultSetHandler<T> rsh, int start, int count) throws SQLException {
		if (dataBaseVersion == null) {
			dataBaseVersion = SQLVersion.getVersionName(conn);
		}
		if (dataBaseVersion == SQLVersion.VersionName.SqlServer) {// 采用Sqlserver分页 jdts1.2.5+sql2005测试 start,count成功
			return limitMSsql(conn, sql, params, rsh, start, start+count);
		} else if (dataBaseVersion == SQLVersion.VersionName.MySql) {// 采用Mysql方式分页
			return limitMysql(conn, sql, params, rsh, start, count);
		} else if(dataBaseVersion == SQLVersion.VersionName.Sqlite){
			return limitSqlLit(conn, sql, params, rsh, start, count);
		}else if (dataBaseVersion == SQLVersion.VersionName.Oracle) {// 采用Oracle方式分页
			return limitOracle(conn, sql, params, rsh, start, start + count);
		}  else {
			throw new SQLException("un support DataBase version to paginQuery!");
		}
	}

	/**
	 * 分页查询
	 * 
	 * @param dataSource
	 *            数据源JNDI
	 * @param countSql
	 *            查询数据总记录的sql
	 * @param queryAllSql
	 *            查询当前页内容的sql
	 * @param params
	 *            查询时的参数
	 * @param rsh
	 *            结果集
	 * @param processor
	 *            行处理类
	 * @param pageNum
	 *            当前页页码
	 * @param maxRow
	 *            每页显示总数
	 * @return Pagin
	 * @throws Exception
	 */
	public Pagin pagin(Connection connection, final String countSql,
			final String queryAllSql, final Object[] params,
			final ResultSetHandler rsh, final RowProcessor processor,
			final int pageNum, final int maxRow) throws SQLException {
		ResultSet rSet = null;
		Statement statement = null;
		PreparedStatement pStatement = null;
		try {
			int maxCount = 0;
			// 不需要传参数
			if (countSql.indexOf("?") == -1
					|| (params == null || params.length < 1)) {
				statement = connection.createStatement();
				rSet = statement.executeQuery(countSql);
				rSet.next();
				maxCount = rSet.getInt(1);
				rSet.close();
			} else {
				pStatement = connection.prepareStatement(countSql,
						ResultSet.TYPE_SCROLL_INSENSITIVE,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				this.fillStatement(pStatement, params);
				rSet = pStatement.executeQuery();
				rSet.next();
				maxCount = rSet.getInt(1);
				rSet.close();
			}
			List list = (List) limit(connection, queryAllSql, params, rsh,
					(pageNum - 1) * maxRow, maxRow);
			return new Pagin(pageNum, maxCount, maxRow, list);
		} catch (SQLException e) {
			e.printStackTrace();
			this.rethrow(e, countSql + "\n" + queryAllSql, params);

		} catch (Exception e) {
			throw new SQLException("分页出错(" + e.getMessage() + ")!", e);

		} finally {
			if (rSet != null) {
				try {
					rSet.close();
				} catch (Exception x) {
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (Exception x) {
				}
			}
			if (pStatement != null) {
				try {
					pStatement.close();
				} catch (Exception x) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception x) {
				}
			}
		}
		return new Pagin(pageNum, maxRow, maxRow, new ArrayList(0));
	}

	public static Long getCount() {
		return count;
	}
}
