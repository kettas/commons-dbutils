/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.dbutils;

import static java.sql.DriverManager.registerDriver;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.dbutils.handlers.ArrayHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.kettas.JDBCPaginRunner;
import org.apache.commons.kettas.NoClobRowProcessor;

/**
 * A collection of JDBC helper methods.  This class is thread safe.
 * 
 */
public final class DbUtils {
	private static String propertiesFile="jdbc.properties";
	private static DataSource ds = null;//默认连接池
	private Connection connection = null;
	// ***********静态变量
	private boolean CloseConnection = true;
	private static boolean CONNCETION_DATASOURCE=true;
	private static JDBCPaginRunner run = new JDBCPaginRunner();// 分页扩展类
	public NoClobRowProcessor rowProcessor=null;
	
    /**
     * Default constructor.
     *
     * Utility classes should not have a public or default constructor,
     * but this one preserves retro-compatibility.
     *
     * @since 1.4
     */
    public DbUtils() {
    	CONNCETION_DATASOURCE=true;
        // do nothing
    }
    public DbUtils(DataSource dataSource) {
    	ds=dataSource;
    	CONNCETION_DATASOURCE=true;
    }
    /**
	 * 处理行
	 */
	private ResultSetHandler getResultBeanHandler(Class cls){
		if(cls == Long.class
				||cls == Integer.class
				||cls == String.class){
			return new ScalarHandler() {
		        protected Object handleRow(ResultSet rs) throws SQLException {  
		            Object obj = super.handle(rs);  
		            if (obj instanceof BigInteger){
		                return ((BigInteger) obj).longValue();
		            }else if(obj instanceof Clob){
		            	return NoClobRowProcessor.filterClob(obj);
		            }else if(obj instanceof Blob){
		            	return NoClobRowProcessor.filterClob(obj);
		            }
		            return obj;  
		        }  
		  
		    }; 
		}
	    return new BeanHandler(cls);
	}
	private ResultSetHandler getResultArrayListHandler(Class cls){
		if(cls == Long.class
				||cls == Integer.class
				||cls == String.class){
			return new ColumnListHandler() {
		        protected Object handleRow(ResultSet rs) throws SQLException {  
		            Object obj = super.handleRow(rs);  
		            if (obj instanceof BigInteger){
		                return ((BigInteger) obj).longValue();
		            }else if(obj instanceof Clob){
		            	return NoClobRowProcessor.filterClob(obj);
		            }else if(obj instanceof Blob){
		            	return NoClobRowProcessor.filterClob(obj);
		            }
		            return obj;  
		        }  
		  
		    }; 
		}
	    return new BeanListHandler(cls);
	}
	private ResultSetHandler getResultBeanListHandler(Class cls){
		if(cls == Long.class
				||cls == Integer.class
				||cls == String.class){
			return new ColumnListHandler() {
		        protected Object handleRow(ResultSet rs) throws SQLException {  
		            Object obj = super.handleRow(rs);  
		            if (obj instanceof BigInteger){
		                return ((BigInteger) obj).longValue();
		            }else if(obj instanceof Clob){
		            	return NoClobRowProcessor.filterClob(obj);
		            }else if(obj instanceof Blob){
		            	return NoClobRowProcessor.filterClob(obj);
		            }
		            return obj;  
		        }  
		  
		    }; 
		}
	    return new BeanListHandler(cls);
	}
	/**
	 * 获得数据库连接（自定义连接是否为只读）
	 * @param readOnly
	 * @return
	 * @throws SQLException
	 */
	private  Connection getConnection(boolean readOnly) throws SQLException {
		if(ds==null){
			ds=getDataSource();
		}
		if(CONNCETION_DATASOURCE
				&& (this.connection == null || connection.isClosed())){
			connection = ds.getConnection();
		}
		if(connection==null){
			throw new SQLException("connection is null ");
		}
//		connection.setReadOnly(readOnly);
		return connection;
	}
	/**
	 * 获得数据库的连接(可读、可写)
	 * @return Connection
	 * @throws SQLException
	 */
	public  Connection getConnection() throws SQLException {
		return getConnection(false);
	}
	/**
	 * 批量执行INSERT,UPDATE,或者DELETE方面的SQL语句
	 * @param sql 需要执行的SQL语句
	 * @param params 需要执行的SQL语句中需要的参数
	 * @return 每条语句执行后更新的条数
	 * @throws SQLException
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public int[] batch(String sql, Object[][] params) throws SQLException {
		int[] rows = null;
		try {
			connection = getConnection();
			rows = run.batch(connection, sql, params);
		} finally {
			close();
		}
		return rows;
	}
	/**
	 * 批量执行INSERT,UPDATE,或者DELETE方面的SQL语句
	 * @param sql 需要执行的SQL
	 * @return 更新行数
	 * @throws SQLException
	 */
	public int executeUpdate(String sql) throws SQLException {
		try {
			connection = getConnection();
			return run.update(connection, sql);
		} finally {
			close();
		}
	}
	public void executeUpdate(String ...sql) throws SQLException {
		try {
			connection = getConnection();
			for(String sqltmp:sql){
				run.update(connection, sqltmp);
			}
		} finally {
			close();
		}
	}
	/**
	 * 批量执行INSERT,UPDATE,或者DELETE方面的SQL语句
	 * @param sql 需要执行的SQL
	 * @param param 参数
	 * @return 更新行数
	 * @throws SQLException
	 */
	public int executeUpdate(String sql, Object param) throws SQLException {
		try {
			connection = getConnection();
			return run.update(connection, sql, param);
		} finally {
			close();
		}
	}
	/**
	 * 批量执行INSERT,UPDATE,或者DELETE方面的SQL语句
	 * @param sql 需要执行的SQL
	 * @param params 参数的数组
	 * @return 更新行数
	 * @throws SQLException
	 */
	public int executeUpdate(String sql, Object[] params) throws SQLException {
		try {
			connection = getConnection();
			return run.update(connection, sql, params);
		} finally {
			close();
		}
	}
	/**
	 * 执行jdbc查询返回javax.sql.rowset.CachedRowSet,该对象是继承java.sql.ResultSet它可以不用关闭，因为在返回结果之前它已经关闭了
	 * @param sql 需要执行的SELECT语句
	 * @return java.sql.RowSet
	 * @throws SQLException
	 */
	public java.sql.ResultSet executeQuery(String sql) throws SQLException {
		return executeQuery(sql,null);
	}
	/**
	 * 执行jdbc查询返回javax.sql.rowset.CachedRowSet,该对象是继承java.sql.ResultSet它可以不用关闭，因为在返回结果之前它已经关闭了
	 * @param sql 需要执行的SELECT语句
	 * @param obj 执行SELECT语句查询时需要的参数
	 * @return java.sql.RowSet
	 * @throws SQLException
	 */
	public java.sql.ResultSet executeQuery(String sql,Object...obj) {
		java.sql.ResultSet rs=null;
		java.sql.PreparedStatement ps=null;
		try {
			connection = getQueryConnection();
			ps=connection.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
			for(int i=0;obj!=null&&i<obj.length;i++){
				ps.setObject(i+1, obj[i]);
			}
			rs=ps.executeQuery();
			javax.sql.rowset.CachedRowSet rowSet=new com.sun.rowset.CachedRowSetImpl();
			rowSet.populate(rs);
			return rowSet;
		}catch(Exception x){
			return null;
		} finally {
			try{
				if(rs!=null){
					closeQuietly(rs);
				}
			}finally{
				if(ps!=null){
					closeQuietly(ps);
				}
				close();
			}
		}
	}
	/**
	 * 获得数据库的连接(可读、不可写)
	 * @return Connection
	 * @throws SQLException
	 */
	public  Connection getQueryConnection()throws SQLException{
		return getConnection(false);
	}
	/**
	 * Execute an SQL SELECT query without any replacement parameters and place
	 * the column values from the first row in an Object[]. Usage Demo:
	 * 
	 * <pre>
	 * Object[] result = searchToArray(sql);
	 * if (result != null) {
	 * 	for (int i = 0; i &lt; result.length; i++) {
	 * 		System.out.println(result[i]);
	 * 	}
	 * }
	 * </pre>
	 * 
	 * @param sql
	 *            The SQL to execute.
	 * @return An Object[] or null if there are no rows in the ResultSet.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public Object[] queryToArray(String sql) throws Exception {
		Connection conn = this.getQueryConnection();
		Object[] result = null;

		ResultSetHandler h = new ArrayHandler(getRowProcessor());
		try {
			result = (Object[]) run.query(conn, sql, h);
		} finally {
			close(conn);
		}

		return result;
	}

	/**
	 * Executes the given SELECT SQL with a single replacement parameter and
	 * place the column values from the first row in an Object[].
	 * 
	 * @param sql
	 *            The SQL statement to execute.
	 * @param param
	 *            The replacement parameter.
	 * @return An Object[] or null if there are no rows in the ResultSet.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public Object[] queryToArray(String sql, Object param) throws Exception {
		Connection conn = this.getQueryConnection();
		Object[] result = null;

		ResultSetHandler h = new ArrayHandler(getRowProcessor());
		try {
			result = (Object[]) run.query(conn, sql, param, h);
		} finally {
			close(conn);
		}

		return result;
	}
	public RowProcessor getRowProcessor() {
		if(rowProcessor==null){
			rowProcessor=new NoClobRowProcessor();
		}
		return rowProcessor;
	}
	/**
	 * Executes the given SELECT SQL query and place the column values from the
	 * first row in an Object[].
	 * 
	 * @param sql
	 *            The SQL statement to execute.
	 * @param params
	 *            Initialize the PreparedStatement's IN parameters with this
	 *            array.
	 * @return An Object[] or null if there are no rows in the ResultSet.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public Object[] queryToArray(String sql, Object[] params) throws Exception {
		Connection conn = this.getQueryConnection();
		Object[] result = null;

		ResultSetHandler h = new ArrayHandler(getRowProcessor());
		try {
			result = (Object[]) run.query(conn, sql, params, h);
		} finally {
			close(conn);
		}
		return result;
	}
	public Object[] queryToArray(String sql, Object[] params,int max) throws Exception {
		return queryToArray(sql, params, 0, max);
	}
	public Object[] queryToArray(String sql, Object[] params,int start,int end) throws Exception {
		Connection conn = this.getQueryConnection();
		ResultSetHandler h = new ArrayHandler(getRowProcessor());
		try {
			return (Object[])run.limit(connection, sql, params, h, start, end);
		} finally {
			close(conn);
		}
	}
	/**
	 * 执行不带任何参数的SQL SELECT查询并将第一条记录反射到指定的bean 
	 * 使用方法:
	 * 
	 * <pre>
	 * String sql = &quot;SELECT * FROM test&quot;;
	 * Test test = (Test) queryToBean(Test.class, sql);
	 * if (test != null) {
	 * 	System.out.println(&quot;test:&quot; + test.getPropertyName());
	 * }
	 * sql = &quot;SELECT id FROM test&quot;;
	 * String ID = (String) queryToBean(String.class, sql);
	 * if (test != null) {
	 * 	System.out.println(&quot;test:&quot; + ID);
	 * }
	 * </pre>
	 * @param type
	 *            The Class of beans.
	 * @param sql
	 *            The SQL to execute.
	 * @return An initialized JavaBean or null if there were no rows in the
	 *         ResultSet.
	 */
	@SuppressWarnings("unchecked")
	public Object queryToBean(Class type, String sql) throws SQLException {
		ResultSetHandler h = getResultBeanHandler(type);
		try {
			connection = this.getQueryConnection();
			return run.query(connection, sql,h);
		} finally {
			close();
		}

	}
	/**
	 * Executes the given SELECT SQL with a single replacement parameter and
	 * Convert the first row of the ResultSet into a bean with the Class given
	 * in the parameter.
	 * 
	 * @param type
	 *            The Class of beans.
	 * @param sql
	 *            The SQL to execute.
	 * @param param
	 *            The replacement parameter.
	 * @return An initialized JavaBean or null if there were no rows in the
	 *         ResultSet.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public Object queryToBean(Class type, String sql, Object param)
			throws SQLException {
		ResultSetHandler h = getResultBeanHandler(type);
		try {
			connection = this.getQueryConnection();
			return run.query(connection, sql,h,param);
		} finally {
			close();
		}
	}

	/**
	 * Executes the given SELECT SQL query and Convert the first row of the
	 * ResultSet into a bean with the Class given in the parameter.
	 * 
	 * @param type
	 *            The Class of beans.
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            Initialize the PreparedStatement's IN parameters with this
	 *            array.
	 * @return An initialized JavaBean or null if there were no rows in the
	 *         ResultSet.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public Object queryToBean(Class type, String sql, Object[] params)
			throws SQLException {
		ResultSetHandler h = getResultBeanHandler(type);
		try {
			connection = this.getQueryConnection();
			return run.query(connection, sql, params,h);
		} finally {
			close();
		}
	}

	/**
	 * Execute an SQL SELECT query without any replacement parameters and
	 * Convert the ResultSet rows into a List of beans with the Class given in
	 * the parameter. Usage Demo:
	 * 
	 * <pre>
	 * ArrayList result = queryToBeanList(Test.class, sql);
	 * Iterator iterator = result.iterator();
	 * while (iterator.hasNext()) {
	 * 	Test test = (Test) iterator.next();
	 * 	System.out.println(test.getPropertyName());
	 * }
	 * sql = &quot;SELECT id FROM test&quot;;
	 * String ID = (String) queryToBean(String.class, sql);
	 * if (test != null) {
	 * 	System.out.println(&quot;test:&quot; + ID);
	 * }
	 * </pre>
	 * 
	 * @param type
	 *            The Class that objects returned from handle() are created
	 *            from.
	 * @param sql
	 *            The SQL to execute.
	 * @return A List of beans (one for each row), never null.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public ArrayList queryToBeanList(Class type, String sql)
			throws SQLException {
		ResultSetHandler h = getResultBeanListHandler(type);
		try {
			connection = this.getQueryConnection();
			return (ArrayList) run.query(connection, sql,h);
		} finally {
			close();
		}
	}

	/**
	 * Executes the given SELECT SQL with a single replacement parameter and
	 * Convert the ResultSet rows into a List of beans with the Class given in
	 * the parameter.
	 * 
	 * @param type
	 *            The Class that objects returned from handle() are created
	 *            from.
	 * @param sql
	 *            The SQL to execute.
	 * @param param
	 *            The replacement parameter.
	 * @return A List of beans (one for each row), never null.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public ArrayList queryToBeanList(Class type, String sql, Object param)
			throws SQLException {
		return (ArrayList) queryToBeanList(type, sql, new Object[]{param});
	}

	/**
	 * Executes the given SELECT SQL query and Convert the ResultSet rows into a
	 * List of beans with the Class given in the parameter.
	 * 
	 * @param type
	 *            The Class that objects returned from handle() are created
	 *            from.
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            Initialize the PreparedStatement's IN parameters with this
	 *            array.
	 * @return A List of beans (one for each row), never null.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public ArrayList queryToBeanList(Class type, String sql, Object[] params)
			throws SQLException {
		ResultSetHandler h = getResultBeanListHandler(type);
		try {
			connection = this.getQueryConnection();
			return (ArrayList) run.query(connection, sql,h,params);
		} finally {
			close();
		}
	}
	/**
	 * Executes the given SELECT SQL query and Convert the ResultSet rows into a
	 * List of beans with the Class given in the parameter.
	 * 
	 * @param type
	 *            The Class that objects returned from handle() are created
	 *            from.
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            Initialize the PreparedStatement's IN parameters with this
	 *            array.
	 * @return A List of beans (one for each row), never null.
	 */
	@SuppressWarnings("unchecked")
	public ArrayList queryToBeanList(Class type, String sql, Object[] params,
			int end) throws SQLException {
		return queryToBeanList(type, sql, params, 0, end);
	}
	/**
	 * Executes the given SELECT SQL query and Convert the ResultSet rows into a
	 * List of beans with the Class given in the parameter.
	 * 
	 * @param type
	 *            The Class that objects returned from handle() are created
	 *            from.
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            Initialize the PreparedStatement's IN parameters with this
	 *            array.
	 * @return A List of beans (one for each row), never null.
	 */
	@SuppressWarnings("unchecked")
	public ArrayList queryToBeanList(Class type, String sql, Object[] params,
			int start, int end) throws SQLException {
		ResultSetHandler h = getResultBeanListHandler(type);
		try {
			connection = this.getQueryConnection();
			return (ArrayList) run.limit(connection, sql, params,h, start, end);
		} finally {
			close();
		}
	}	
	/**
	 * Execute an SQL SELECT query without any replacement parameters and
	 * converts the first ResultSet into a Map object. Usage Demo:
	 * 
	 * <pre>
	 * Map result = queryToMap(sql);
	 * System.out.println(map.get(columnName));
	 * </pre>
	 * 
	 * @param sql
	 *            The SQL to execute.
	 * @return A Map with the values from the first row or null if there are no
	 *         rows in the ResultSet.
	 */
	@SuppressWarnings("unchecked")
	public Map queryToMap(String sql) throws SQLException {
		ResultSetHandler h = new MapHandler(getRowProcessor());
		try {
			connection = this.getQueryConnection();
			return (Map) run.query(connection, sql, h);
		} finally {
			close();
		}
	}
	/**
	 * Executes the given SELECT SQL with a single replacement parameter and
	 * converts the first ResultSet into a Map object.
	 * 
	 * @param sql
	 *            The SQL to execute.
	 * @param param
	 *            The replacement parameter.
	 * @return A Map with the values from the first row or null if there are no
	 *         rows in the ResultSet.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public Map queryToMap(String sql, Object param) throws SQLException {
		ResultSetHandler h = new MapHandler(getRowProcessor());
		try {
			connection = this.getQueryConnection();
			if(param!=null&&param instanceof List){
				return (Map) run.query(connection, sql, h,new Object[]{param}, h);
			}
			return (Map) run.query(connection, sql, h,param);
		} finally {
			closeQuietly(connection);
		}
	}
	/**
	 * Executes the given SELECT SQL query and converts the first ResultSet into
	 * a Map object.
	 * 
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            Initialize the PreparedStatement's IN parameters with this
	 *            array.
	 * @return A Map with the values from the first row or null if there are no
	 *         rows in the ResultSet.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public Map queryToMap(String sql, Object[] params) throws SQLException {
		Map result = null;
		ResultSetHandler h = new MapHandler(getRowProcessor());
		try {
			connection = this.getQueryConnection();
			result = (Map) run.query(connection, sql, params, h);
		} finally {
			close();
		}

		return result;
	}
	/**
	 * Executes the given SELECT SQL with a single replacement parameter and
	 * converts the ResultSet into a List of Map objects.
	 * 
	 * @param sql
	 *            The SQL to execute.
	 * @param param
	 *            The replacement parameter.
	 * @return A List of Maps, never null.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public List queryToMapList(String sql, Object param) throws SQLException {
		ResultSetHandler h = new MapListHandler(getRowProcessor());
		try {
			connection = this.getQueryConnection();
			return (List) run.query(connection, sql, h,param);
		} finally {
			close();
		}
	}
	public List queryToMapList(String sql) throws SQLException {
		ResultSetHandler h = new MapListHandler(getRowProcessor());
		try {
			connection = this.getQueryConnection();
			return (List) run.query(connection, sql, h);
		} finally {
			close();
		}
	}
	/**
	 * 执行给定的SELECT SQL查询，并将其ResultSet转换成的List of Map objects.
	 * 
	 * @param sql
	 *            执行的Sql
	 * @param params
	 *            用此数据补充Sql中的参数
	 * @return A List of Maps, never null.
	 */
	@SuppressWarnings({"unchecked", "deprecation"})
	public List queryToMapList(String sql, Object[] params) throws SQLException {
		ResultSetHandler h = new MapListHandler(getRowProcessor());
		try {
			connection = this.getQueryConnection();
			return (List) run.query(connection, sql, params, h);
		} finally {
			close();
		}
	}
	/**
	 * 执行给定的SELECT SQL查询,返回指定的指针之间数据,并将其ResultSet转换成的List of Map objects.
	 * 
	 * @param sql
	 *            执行的Sql
	 * @param params
	 *            用此数据补充Sql中的参数
	 * @param start
	 *            用此数组表示数据开始的指针
	 * @param maxRow
	 *            用此数组表示数据结束的指针
	 * @return A List of Maps, never null.
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	public List queryToMapList(String sql, Object[] params, int start,
			int maxRow) throws SQLException {
		try {
			ResultSetHandler h = new MapListHandler(new NoClobRowProcessor());
			connection = this.getQueryConnection();
			return (List) run.limit(connection, sql, params, h, start, maxRow);
		} finally {
			close();
		}
	}
	/**
	 * 执行给定的SELECT SQL查询,返回指定的指针最前n条数据,并将其ResultSet转换成的List of Map objects.
	 * 
	 * @param sql
	 *            执行的Sql
	 * @param params
	 *            用此数据补充Sql中的参数
	 * @param maxRow 国
	 * @return ArrayList
	 * @throws Exception
	 */
	public List queryToMapList(String sql, Object[] params, int maxRow)
			throws SQLException {
		return queryToMapList(sql, params, 0, maxRow);
	}
	/**
	 * 带参数的分页查询
	 * @param pageNum 需要查询页码页码
	 * @param maxRow 分页时每页的最大允许显示的总数
	 * @param countSql 分页时统计信息总数的sql语句(select count(*) from table1)
	 * @param queryAllSql 分页时显示当前页内容的sql
	 * @param params sql中需要的参数(允许为空)
	 * @return Pagin
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public Pagin pagin(int pageNum,int maxRow,String countSql,String queryAllSql,Object...params)throws Exception{
		ResultSetHandler h = new MapListHandler(getRowProcessor());
		return run.pagin(getQueryConnection(), countSql, queryAllSql, params,h,getRowProcessor(), pageNum, maxRow);
	}
	/**
	 * 不带参数的分页查询
	 * @param pageNum 需要查询页码页码
	 * @param maxRow 分页时每页的最大允许显示的总数
	 * @param countSql 分页时统计信息总数的sql语句(select count(*) from table1)
	 * @param queryAllSql 用于查询全部的SQL(一定是查询全部如select * from table,底层已经对分页作优化不会加载多余数据)
	 * @return Pagin
	 * @throws Exception
	 */
	public Pagin pagin(int pageNum,int maxRow,String countSql,String queryAllSql)throws Exception{
		ResultSetHandler h = new MapListHandler(getRowProcessor());
		return run.pagin(getQueryConnection(), countSql, queryAllSql, null,h,getRowProcessor(), pageNum, maxRow);
	}
	public void refresh(){
		close();
		ds=null;
		CloseConnection = true;
		CONNCETION_DATASOURCE=true;
	}
	/**
	 * 强行关闭本身尚未关闭的数据库连接
     */
	public void close() {
		try{
			if(CloseConnection && connection != null){
				connection.close();
			}
		}catch(SQLException e){
			//Quit
		}
	}
	/**
	 * 关闭本身尚未关闭的数据库连接
	 * @param close 是否强制关闭
	 */
	public void close(boolean close) {
		CloseConnection = close;
		close();
	}
    /**
     * Close a <code>Connection</code>, avoid closing if null.
     *
     * @param conn Connection to close.
     * @throws SQLException if a database access error occurs
     */
    public static void close(Connection conn) throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * Close a <code>ResultSet</code>, avoid closing if null.
     *
     * @param rs ResultSet to close.
     * @throws SQLException if a database access error occurs
     */
    public static void close(ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
    }

    /**
     * Close a <code>Statement</code>, avoid closing if null.
     *
     * @param stmt Statement to close.
     * @throws SQLException if a database access error occurs
     */
    public static void close(Statement stmt) throws SQLException {
        if (stmt != null) {
            stmt.close();
        }
    }

    /**
     * Close a <code>Connection</code>, avoid closing if null and hide
     * any SQLExceptions that occur.
     *
     * @param conn Connection to close.
     */
    public static void closeQuietly(Connection conn) {
        try {
            close(conn);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    /**
     * Close a <code>Connection</code>, <code>Statement</code> and
     * <code>ResultSet</code>.  Avoid closing if null and hide any
     * SQLExceptions that occur.
     *
     * @param conn Connection to close.
     * @param stmt Statement to close.
     * @param rs ResultSet to close.
     */
    public static void closeQuietly(Connection conn, Statement stmt,
            ResultSet rs) {

        try {
            closeQuietly(rs);
        } finally {
            try {
                closeQuietly(stmt);
            } finally {
                closeQuietly(conn);
            }
        }

    }

    /**
     * Close a <code>ResultSet</code>, avoid closing if null and hide any
     * SQLExceptions that occur.
     *
     * @param rs ResultSet to close.
     */
    public static void closeQuietly(ResultSet rs) {
        try {
            close(rs);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    /**
     * Close a <code>Statement</code>, avoid closing if null and hide
     * any SQLExceptions that occur.
     *
     * @param stmt Statement to close.
     */
    public static void closeQuietly(Statement stmt) {
        try {
            close(stmt);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    /**
     * Commits a <code>Connection</code> then closes it, avoid closing if null.
     *
     * @param conn Connection to close.
     * @throws SQLException if a database access error occurs
     */
    public static void commitAndClose(Connection conn) throws SQLException {
        if (conn != null) {
            try {
                conn.commit();
            } finally {
                conn.close();
            }
        }
    }

    /**
     * Commits a <code>Connection</code> then closes it, avoid closing if null
     * and hide any SQLExceptions that occur.
     *
     * @param conn Connection to close.
     */
    public static void commitAndCloseQuietly(Connection conn) {
        try {
            commitAndClose(conn);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    /**
     * Loads and registers a database driver class.
     * If this succeeds, it returns true, else it returns false.
     *
     * @param driverClassName of driver to load
     * @return boolean <code>true</code> if the driver was found, otherwise <code>false</code>
     */
    public static boolean loadDriver(String driverClassName) {
        return loadDriver(DbUtils.class.getClassLoader(), driverClassName);
    }
    /**
     * 获得Dbcp连接池(jdbc.properties);也支持(jndl中间件连接池)
     * @return DataSource
     * @throws Exception 
     */
    public static DataSource getDataSource() {
    	Properties properties=new Properties();
		try{
			properties.load(DbUtils.class.getClassLoader().getResourceAsStream(propertiesFile));
			return getDataSource(properties);
		}catch(Exception x){
			x.printStackTrace();
			return null;
		}
    }
	/**
     * 获得中间件连接池
     * @param name
     * @return DataSource
     * @throws Exception
     */
	public static DataSource getDataSource(String name)throws Exception{
		return (DataSource) new InitialContext().lookup(name);
	}
	public static DataSource getDataSource(Properties properties)throws Exception{
		if(properties.containsKey("jndl")){
			return getDataSource(properties.getProperty("jndl"));
		}else{
			return BasicDataSourceFactory.createDataSource(properties);
		}
	}
    /**
     * Loads and registers a database driver class.
     * If this succeeds, it returns true, else it returns false.
     *
     * @param classLoader the class loader used to load the driver class
     * @param driverClassName of driver to load
     * @return boolean <code>true</code> if the driver was found, otherwise <code>false</code>
     * @since 1.4
     */
    public static boolean loadDriver(ClassLoader classLoader, String driverClassName) {
        try {
            Class<?> loadedClass = classLoader.loadClass(driverClassName);

            if (!Driver.class.isAssignableFrom(loadedClass)) {
                return false;
            }

            @SuppressWarnings("unchecked") // guarded by previous check
            Class<Driver> driverClass = (Class<Driver>) loadedClass;
            Constructor<Driver> driverConstructor = driverClass.getConstructor();

            // make Constructor accessible if it is private
            boolean isConstructorAccessible = driverConstructor.isAccessible();
            if (!isConstructorAccessible) {
                driverConstructor.setAccessible(true);
            }

            try {
                Driver driver = driverConstructor.newInstance();
                registerDriver(new DriverProxy(driver));
            } finally {
                driverConstructor.setAccessible(isConstructorAccessible);
            }

            return true;
        } catch (RuntimeException e) {
            return false;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Print the stack trace for a SQLException to STDERR.
     *
     * @param e SQLException to print stack trace of
     */
    public static void printStackTrace(SQLException e) {
        printStackTrace(e, new PrintWriter(System.err));
    }

    /**
     * Print the stack trace for a SQLException to a
     * specified PrintWriter.
     *
     * @param e SQLException to print stack trace of
     * @param pw PrintWriter to print to
     */
    public static void printStackTrace(SQLException e, PrintWriter pw) {

        SQLException next = e;
        while (next != null) {
            next.printStackTrace(pw);
            next = next.getNextException();
            if (next != null) {
                pw.println("Next SQLException:");
            }
        }
    }

    /**
     * Print warnings on a Connection to STDERR.
     *
     * @param conn Connection to print warnings from
     */
    public static void printWarnings(Connection conn) {
        printWarnings(conn, new PrintWriter(System.err));
    }

    /**
     * Print warnings on a Connection to a specified PrintWriter.
     *
     * @param conn Connection to print warnings from
     * @param pw PrintWriter to print to
     */
    public static void printWarnings(Connection conn, PrintWriter pw) {
        if (conn != null) {
            try {
                printStackTrace(conn.getWarnings(), pw);
            } catch (SQLException e) {
                printStackTrace(e, pw);
            }
        }
    }

    /**
     * Rollback any changes made on the given connection.
     * @param conn Connection to rollback.  A null value is legal.
     * @throws SQLException if a database access error occurs
     */
    public static void rollback(Connection conn) throws SQLException {
        if (conn != null) {
            conn.rollback();
        }
    }

    /**
     * Performs a rollback on the <code>Connection</code> then closes it,
     * avoid closing if null.
     *
     * @param conn Connection to rollback.  A null value is legal.
     * @throws SQLException if a database access error occurs
     * @since DbUtils 1.1
     */
    public static void rollbackAndClose(Connection conn) throws SQLException {
        if (conn != null) {
            try {
                conn.rollback();
            } finally {
                conn.close();
            }
        }
    }

    /**
     * Performs a rollback on the <code>Connection</code> then closes it,
     * avoid closing if null and hide any SQLExceptions that occur.
     *
     * @param conn Connection to rollback.  A null value is legal.
     * @since DbUtils 1.1
     */
    public static void rollbackAndCloseQuietly(Connection conn) {
        try {
            rollbackAndClose(conn);
        } catch (SQLException e) { // NOPMD
            // quiet
        }
    }

    /**
     * Simple {@link Driver} proxy class that proxies a JDBC Driver loaded dynamically.
     *
     * @since 1.6
     */
    private static final class DriverProxy implements Driver {

        private boolean parentLoggerSupported = true;

        /**
         * The adapted JDBC Driver loaded dynamically.
         */
        private final Driver adapted;

        /**
         * Creates a new JDBC Driver that adapts a JDBC Driver loaded dynamically.
         *
         * @param adapted the adapted JDBC Driver loaded dynamically.
         */
        public DriverProxy(Driver adapted) {
            this.adapted = adapted;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean acceptsURL(String url) throws SQLException {
            return adapted.acceptsURL(url);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            return adapted.connect(url, info);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getMajorVersion() {
            return adapted.getMajorVersion();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getMinorVersion() {
            return adapted.getMinorVersion();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
            return adapted.getPropertyInfo(url, info);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean jdbcCompliant() {
            return adapted.jdbcCompliant();
        }

        /**
         * Java 1.7 method.
         */
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            if (parentLoggerSupported) {
                try {
                    Method method = adapted.getClass().getMethod("getParentLogger", new Class[0]);
                    return (Logger)method.invoke(adapted, new Object[0]);
                } catch (NoSuchMethodException e) {
                    parentLoggerSupported = false;
                    throw new SQLFeatureNotSupportedException(e);
                } catch (IllegalAccessException e) {
                    parentLoggerSupported = false;
                    throw new SQLFeatureNotSupportedException(e);
                } catch (InvocationTargetException e) {
                    parentLoggerSupported = false;
                    throw new SQLFeatureNotSupportedException(e);
                }
            }
            throw new SQLFeatureNotSupportedException();
        }

    }

	public static String getPropertiesFile() {
		return propertiesFile;
	}
	public static void setPropertiesFile(String propertiesFile) {
		DbUtils.propertiesFile = propertiesFile;
	}

	public void setColumnNameToLowerCase(){
		rowProcessor.setKeyNameType(1);
	}
}
