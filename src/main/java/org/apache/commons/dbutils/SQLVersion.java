package org.apache.commons.dbutils;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据库版本控制类
 * @author kettas
 * 3:24:04 PM
 */
public class SQLVersion {
	public static Map dbInfo=null;
	
	private static void checkDbType(java.sql.Connection connection){
		if(dbInfo==null){
			Map map=new HashMap();
			try{
				DatabaseMetaData metaData = connection.getMetaData();
				map.put("databaseName", metaData.getDatabaseProductName());
				map.put("VersionName", metaData.getDatabaseProductVersion());
				map.put("driverName", metaData.getDriverName());
				map.put("driverVersion", metaData.getDriverVersion());
				map.put("userName",metaData.getUserName());
				map.put("url", metaData.getURL());
				map.put("maxConnection", metaData.getMaxConnections());
			}catch (Exception e) {
				e.printStackTrace();
			}
			dbInfo=map;
		}
	}
	/**
	 * 支持的数据库版本枚举�?
	 * @author kettas
	 * 3:03:29 PM
	 */
	public enum VersionName {
        SqlServer,Oracle,MySql,Sqlite;
    }
	//判断数据库的采用的类�?
	public static String getVersion(javax.sql.DataSource datasource)throws SQLException{
		java.sql.Connection con=null;
		try{
			con=datasource.getConnection();
			return getVersion(con);
		}finally{
			if(con!=null){
				con.close();
			}
		}
	}
	
	//判断数据库的采用的类�?
	public static String getVersion(java.sql.Connection connection)throws SQLException{
		try{
			if(dbInfo!=null){
				return String.valueOf(dbInfo.get("databaseName"));
			}else{
				checkDbType(connection);
			}
			return String.valueOf(dbInfo!=null?dbInfo.get("databaseName"):"");
		}catch (NullPointerException e) {
			e.printStackTrace();
			throw new SQLException("unknow database.");
		}
	}
	/**
	 * 获得数据库版�?(通过JDBC获得)
	 * @return SQLVersion.VersionName
	 */
	public static VersionName getVersionName(javax.sql.DataSource datasource) {
		java.sql.Connection con=null;
		try{
			con=datasource.getConnection();
			return getVersionName(con);
		}catch(Exception x){
		   x.printStackTrace();
		}finally{
			DbUtils.closeQuietly(con);
		}
		return SQLVersion.VersionName.SqlServer;
	}
	/**
	 * 获得数据库版(通过JDBC获得)注意，此方法执行后不会关闭连
	 * @param connection
	 * @return SQLVersion.VersionName
	 */
	public static VersionName getVersionName(java.sql.Connection connection)throws java.sql.SQLException {
		String versionNameString=getVersion(connection).toLowerCase();
		versionNameString=versionNameString==null?"":versionNameString;
		if(versionNameString.indexOf("microsoft")>-1){
			return SQLVersion.VersionName.SqlServer;
		}else if(versionNameString.indexOf("oracle")>-1){
			return SQLVersion.VersionName.Oracle;
		}else if(versionNameString.indexOf("mysql")>-1){
			return SQLVersion.VersionName.MySql;
		}else if(versionNameString.indexOf("sqlite")>-1){
			return SQLVersion.VersionName.Sqlite;
		}
		throw new SQLException("不明类型("+versionNameString+")");
	}
}
