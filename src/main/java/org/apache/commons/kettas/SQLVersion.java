package org.apache.commons.kettas;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;

/**
 * 数据库版本控制类
 * @author kettas
 * 3:24:04 PM
 */
public class SQLVersion {
	public static Map dbInfo=null;
	
	private static void checkDbType(java.sql.DatabaseMetaData dataBaseMetaData){
		try {
			if(dbInfo==null||!dataBaseMetaData.getURL().equalsIgnoreCase(String.valueOf(dbInfo.get("url")))){
				Map map=new HashMap();
				try{
					map.put("databaseName", dataBaseMetaData.getDatabaseProductName());
					map.put("VersionName", dataBaseMetaData.getDatabaseProductVersion());
					map.put("driverName", dataBaseMetaData.getDriverName());
					map.put("driverVersion", dataBaseMetaData.getDriverVersion());
					map.put("userName",dataBaseMetaData.getUserName());
					map.put("url", dataBaseMetaData.getURL());
					map.put("maxConnection", dataBaseMetaData.getMaxConnections());
				}catch (Exception e) {
					e.printStackTrace();
				}
				dbInfo=map;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 支持的数据库版本枚举类型
	 * @author kettas
	 * 3:03:29 PM
	 */
	public enum VersionName {
        SqlServer,Oracle,MySql,Sqlite;
    }
	
	//判断数据库的采用的类型
	public static String getVersion(java.sql.DatabaseMetaData dataBaseMetaData)throws SQLException{
		try{
			checkDbType(dataBaseMetaData);
			return String.valueOf(dbInfo!=null?dbInfo.get("databaseName"):"");
		}catch (NullPointerException e) {
			e.printStackTrace();
			throw new SQLException("unknow database.");
		}
	}
	/**
	 * 获得数据库版本(通过JDBC获得)
	 * @return SQLVersion.VersionName
	 */
	public static VersionName getVersionName(javax.sql.DataSource datasource) {
		java.sql.Connection con=null;
		try{
			con=datasource.getConnection();
			return getVersionName(con.getMetaData());
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
	public static VersionName getVersionName(java.sql.DatabaseMetaData dataBaseMetaData)throws java.sql.SQLException {
		String versionNameString=getVersion(dataBaseMetaData).toLowerCase();
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
