package org.apache.commons.kettas;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.BeanProcessor;
import org.apache.commons.dbutils.RowProcessor;
/**
 * 过滤Clob字段,处理Clob字段查询的时候，全部都将其转换为String对象，希望能够统一数据库
 * 如果发现有新的字段导致数据库不能统一再扩展。
 * @author 杨伦亮
 * 上午12:47:59
 */
public class NoClobRowProcessor implements RowProcessor {
    /**
     * The default BeanProcessor instance to use if not supplied in the
     * constructor.
     */
    private static final BeanProcessor defaultConvert = new BeanProcessor();

    /**
     * The Singleton instance of this class.
     */
    private static final BasicRowProcessor instance = new BasicRowProcessor();
    private int keyNameType=0;//返回的字段名类型:[0自动|1小定|2大写]
    /**
     * 返回的字段名类型:[0自动|1小定|2大写]
     * @param keyNameType
     */
    public void setKeyNameType(int keyNameType) {
		this.keyNameType = keyNameType;
	}

	/**
     * Returns the Singleton instance of this class.
     *
     * @return The single instance of this class.
     * @deprecated Create instances with the constructors instead.  This will 
     * be removed after DbUtils 1.1.
     */
    public static BasicRowProcessor instance() {
        return instance;
    }

    /**
     * Use this to process beans.
     */
    private final BeanProcessor convert;

    /**
     * BasicRowProcessor constructor.  Bean processing defaults to a 
     * BeanProcessor instance.
     */
    public NoClobRowProcessor() {
        this(defaultConvert);
    }
    
    /**
     * BasicRowProcessor constructor.
     * @param convert The BeanProcessor to use when converting columns to 
     * bean properties.
     * @since DbUtils 1.1
     */
    public NoClobRowProcessor(BeanProcessor convert) {
        super();
        this.convert = convert;
    }
    /**
     * 支持分页功能
     * @param convert 转换接口
     * @param backMaxRow 返回最大记录
     */
    public NoClobRowProcessor(BeanProcessor convert,int backMaxRow){
    	 super();
         this.convert = convert;
    }
    /**
     * 支持分页功能
     * @param backMaxRow 返回最大记录
     */
    public NoClobRowProcessor(int backMaxRow){
    	 super();
    	 this.convert = defaultConvert;
    }
    /**
     * 过滤clob
     * @param object
     * @return
     */
    public static Object filterClob(Object object){
    	try{
    		//过滤Clob
        	if(object instanceof Clob){
    			Clob clob=(Clob)object;
    			Reader is = clob.getCharacterStream();// 得到流
    			BufferedReader br = new BufferedReader(is);
    			String s = br.readLine();
    			StringBuffer sb = new StringBuffer();
    			while (s != null) {// 执行循环将字符串全部取出付值给StringBuffer由StringBuffer转成STRING
    				sb.append(s);
    				s = br.readLine();
    			}
    			is.close();
    			return sb.toString();
    		//过滤Blob
    		}else if(object instanceof Blob){
    			Blob clob=(Blob)object;
    			byte [] body=new byte[Integer.parseInt(String.valueOf(clob.length()))];
    			InputStream is = clob.getBinaryStream();// 得到流
    			is.read(body);
    			is.close();
//    			clob.free();
    			return body;
    		}
    	}catch (Exception e) {
    		e.printStackTrace();
    	}
    	return object;
    }
    /**
     * Convert a <code>ResultSet</code> row into an <code>Object[]</code>.
     * This implementation copies column values into the array in the same 
     * order they're returned from the <code>ResultSet</code>.  Array elements
     * will be set to <code>null</code> if the column was SQL NULL.
     *
     * @see org.apache.commons.dbutils.RowProcessor#toArray(java.sql.ResultSet)
     * @param rs ResultSet that supplies the array data
     * @throws SQLException if a database access error occurs
     * @return the newly created array
     */
    public Object[] toArray(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        Object[] result = new Object[cols];
        for (int i = 0; i < cols; i++) {
            result[i] = filterClob(rs.getObject(i + 1));
        }

        return result;
    }

    /**
     * Convert a <code>ResultSet</code> row into a JavaBean.  This 
     * implementation delegates to a BeanProcessor instance.
     * @see org.apache.commons.dbutils.RowProcessor#toBean(java.sql.ResultSet, java.lang.Class)
     * @see org.apache.commons.dbutils.BeanProcessor#toBean(java.sql.ResultSet, java.lang.Class)
     * @param <T> The type of bean to create
     * @param rs ResultSet that supplies the bean data
     * @param type Class from which to create the bean instance
     * @throws SQLException if a database access error occurs
     * @return the newly created bean 
     */
    public <T> T toBean(ResultSet rs, Class<? extends T> type) throws SQLException {
        return this.convert.toBean(rs, type);
    }

    /**
     * Convert a <code>ResultSet</code> into a <code>List</code> of JavaBeans.  
     * This implementation delegates to a BeanProcessor instance. 
     * @see org.apache.commons.dbutils.RowProcessor#toBeanList(java.sql.ResultSet, java.lang.Class)
     * @see org.apache.commons.dbutils.BeanProcessor#toBeanList(java.sql.ResultSet, java.lang.Class)
     * @param <T> The type of bean to create
     * @param rs ResultSet that supplies the bean data
     * @param type Class from which to create the bean instance
     * @throws SQLException if a database access error occurs
     * @return A <code>List</code> of beans with the given type in the order 
     * they were returned by the <code>ResultSet</code>.
     */
    public <T> List<T> toBeanList(ResultSet rs, Class<? extends T> type) throws SQLException {
        return this.convert.toBeanList(rs, type);
    }

    /**
     * Convert a <code>ResultSet</code> row into a <code>Map</code>.  This 
     * implementation returns a <code>Map</code> with case insensitive column
     * names as keys.  Calls to <code>map.get("COL")</code> and 
     * <code>map.get("col")</code> return the same value.
     * @see org.apache.commons.dbutils.RowProcessor#toMap(java.sql.ResultSet)
     * @param rs ResultSet that supplies the map data
     * @throws SQLException if a database access error occurs
     * @return the newly created Map
     */
    public Map<String, Object> toMap(ResultSet rs) throws SQLException {
        Map<String, Object> result = new CaseInsensitiveHashMap();
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();
        for (int i = 1; i <= cols; i++) {
            if(this.keyNameType==0){
                result.put(rsmd.getColumnName(i),filterClob(rs.getObject(i)));
            }else if(this.keyNameType==2){
            	result.put(rsmd.getColumnName(i).toUpperCase(),filterClob(rs.getObject(i)));
            }else if(this.keyNameType==1){
            	result.put(rsmd.getColumnName(i).toLowerCase(),filterClob(rs.getObject(i)));
            }else{
            	result.put(rsmd.getColumnName(i),filterClob(rs.getObject(i)));
            }
        }
        return result;
    }
    /**
     * Convert all <code>ResultSet</code>  into a <code>List of Map</code>.  
     * @param rs ResultSet that supplies the map data
     * @return the newly created List
     * @throws SQLException
     */
    public List<Map<String, Object>> toMapList(ResultSet rs) throws SQLException {
        List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
        while(rs.next()){
        	list.add(toMap(rs));
        }
        return list;
    }
    /**
     * Convert a <code>ResultSet</code> row into a <code>Map</code>.  This 
     * implementation returns a <code>Map</code> with case insensitive column
     * names as keys.  Calls to <code>map.get("COL")</code> and 
     * <code>map.get("col")</code> return the same value.
     * @see org.apache.commons.dbutils.RowProcessor#toMap(java.sql.ResultSet)
     * @param rs ResultSet that supplies the map data
     * @throws SQLException if a database access error occurs
     * @return the newly created Map
     */
    public Map<String, Object> toMap(javax.sql.RowSet rs) throws SQLException {
        Map<String, Object> result = new CaseInsensitiveHashMap();
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            result.put(rsmd.getColumnName(i),filterClob(rs.getObject(i)));
        }

        return result;
    }
    /**
     * A Map that converts all keys to lowercase Strings for case insensitive
     * lookups.  This is needed for the toMap() implementation because 
     * databases don't consistenly handle the casing of column names. 
     * 
     * <p>The keys are stored as they are given [BUG #DBUTILS-34], so we maintain
     * an internal mapping from lowercase keys to the real keys in order to 
     * achieve the case insensitive lookup.
     * 
     * <p>Note: This implementation does not allow <tt>null</tt>
     * for key, whereas {@link HashMap} does, because of the code:
     * <pre>
     * key.toString().toLowerCase()
     * </pre>
     */
    private static class CaseInsensitiveHashMap extends HashMap<String, Object> {
        /**
         * The internal mapping from lowercase keys to the real keys.
         * 
         * <p>
         * Any query operation using the key 
         * ({@link #get(Object)}, {@link #containsKey(Object)})
         * is done in three steps:
         * <ul>
         * <li>convert the parameter key to lower case</li>
         * <li>get the actual key that corresponds to the lower case key</li>
         * <li>query the map with the actual key</li>
         * </ul>
         * </p>
         */
        private final Map<String,String> lowerCaseMap = new HashMap<String,String>();

        /**
         * Required for serialization support.
         * 
         * @see java.io.Serializable
         */ 
        private static final long serialVersionUID = -2848100435296897392L;

        /** {@inheritDoc} */
        @Override
        public boolean containsKey(Object key) {
            Object realKey = lowerCaseMap.get(key.toString().toLowerCase());
            return super.containsKey(realKey);
            // Possible optimisation here:
            // Since the lowerCaseMap contains a mapping for all the keys,
            // we could just do this:
            // return lowerCaseMap.containsKey(key.toString().toLowerCase());
        }

        /** {@inheritDoc} */
        @Override
        public Object get(Object key) {
            Object realKey = lowerCaseMap.get(key.toString().toLowerCase());
            return super.get(realKey);
        }
       
        /** {@inheritDoc} */
        @Override
        public Object put(String key, Object value) {
            /*
             * In order to keep the map and lowerCaseMap synchronized,
             * we have to remove the old mapping before putting the 
             * new one. Indeed, oldKey and key are not necessaliry equals.
             * (That's why we call super.remove(oldKey) and not just
             * super.put(key, value))
             */
            Object oldKey = lowerCaseMap.put(key.toLowerCase(), key);
            Object oldValue = super.remove(oldKey);
            
            super.put(key, value);
            return oldValue;
        }

        /** {@inheritDoc} */
        @Override
        public void putAll(Map<? extends String,?> m) {
            for (Map.Entry<? extends String, ?> entry : m.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                this.put(key, value);
            }
        }

        /** {@inheritDoc} */
        @Override
        public Object remove(Object key) {
            Object realKey = lowerCaseMap.remove(key.toString().toLowerCase());
            return super.remove(realKey);
        }
    }
    
}
