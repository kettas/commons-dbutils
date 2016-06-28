package org.apache.commons.kettas;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

public class HandlerUtil {
	private static String trimAllWhitespace(String str) {
		if (str==null||str.trim().length()<1) {
			return str;
		}
		StringBuffer buf = new StringBuffer(str);
		int index = 0;
		while (buf.length() > index) {
			if (Character.isWhitespace(buf.charAt(index))) {
				buf.deleteCharAt(index);
			}

			++index;
		}

		return buf.toString();
	}
	private static boolean isHexNumber(String value) {
		int index = (value.startsWith("-")) ? 1 : 0;
		return ((value.startsWith("0x", index))
				|| (value.startsWith("0X", index)) || (value.startsWith("#",
				index)));
	}
	public static Object handleRow(Object obj,Class targetClass) throws SQLException {  
		//取值
		if(obj instanceof String){
			obj=String.valueOf(obj);
		}else if(obj instanceof Clob){
			obj= NoClobRowProcessor.filterClob(obj);
        }else if(obj instanceof Blob){
        	obj= NoClobRowProcessor.filterClob(obj);
        }
		//格式化字符
		if (targetClass.equals(String.class)) {
			return obj;
		}
		
		//格式化数字
		String trimmed=obj==null?"":trimAllWhitespace(String.valueOf(obj));
		if (targetClass.equals(Byte.class)) {
			return ((isHexNumber(trimmed)) ? Byte.decode(trimmed) : Byte
					.valueOf(trimmed));
		}
		if (targetClass.equals(Short.class)) {
			return ((isHexNumber(trimmed)) ? Short.decode(trimmed) : Short
					.valueOf(trimmed));
		}
		if (targetClass.equals(Integer.class)) {
			return ((isHexNumber(trimmed)) ? Integer.decode(trimmed) : Integer
					.valueOf(trimmed));
		}
		if (targetClass.equals(Long.class)) {
			return ((isHexNumber(trimmed)) ? Long.decode(trimmed) : Long
					.valueOf(trimmed));
		}
		if (targetClass.equals(BigInteger.class)) {
			return new BigInteger(trimmed);
		}
		if (targetClass.equals(Float.class)) {
			return Float.valueOf(trimmed);
		}
		if (targetClass.equals(Double.class)) {
			return Double.valueOf(trimmed);
		}
		if ((targetClass.equals(BigDecimal.class))
				|| (targetClass.equals(Number.class))) {
			return new BigDecimal(trimmed);
		}
		//格式化布尔值
		if ((targetClass.equals(Boolean.class))){
			if ("true".equals(trimmed)) {
				return Boolean.TRUE;
			} else if (trimmed == null) {
				return null;
			} else {
				char ch0;
				char ch1;
				char ch2;
				char ch3;
				switch (trimmed.length()) {
				case 1:
					ch0 = trimmed.charAt(0);
					if (ch0 == 121 || ch0 == 89 || ch0 == 116 || ch0 == 84) {
						return Boolean.TRUE;
					}

					if (ch0 != 110 && ch0 != 78 && ch0 != 102 && ch0 != 70) {
						break;
					}

					return Boolean.FALSE;
				case 2:
					ch0 = trimmed.charAt(0);
					ch1 = trimmed.charAt(1);
					if ((ch0 == 111 || ch0 == 79) && (ch1 == 110 || ch1 == 78)) {
						return Boolean.TRUE;
					}

					if ((ch0 == 110 || ch0 == 78) && (ch1 == 111 || ch1 == 79)) {
						return Boolean.FALSE;
					}
					break;
				case 3:
					ch0 = trimmed.charAt(0);
					ch1 = trimmed.charAt(1);
					ch2 = trimmed.charAt(2);
					if ((ch0 == 121 || ch0 == 89) && (ch1 == 101 || ch1 == 69) && (ch2 == 115 || ch2 == 83)) {
						return Boolean.TRUE;
					}

					if ((ch0 == 111 || ch0 == 79) && (ch1 == 102 || ch1 == 70) && (ch2 == 102 || ch2 == 70)) {
						return Boolean.FALSE;
					}
					break;
				case 4:
					ch0 = trimmed.charAt(0);
					ch1 = trimmed.charAt(1);
					ch2 = trimmed.charAt(2);
					ch3 = trimmed.charAt(3);
					if ((ch0 == 116 || ch0 == 84) && (ch1 == 114 || ch1 == 82) && (ch2 == 117 || ch2 == 85)
							&& (ch3 == 101 || ch3 == 69)) {
						return Boolean.TRUE;
					}
					break;
				case 5:
					ch0 = trimmed.charAt(0);
					ch1 = trimmed.charAt(1);
					ch2 = trimmed.charAt(2);
					ch3 = trimmed.charAt(3);
					char ch4 = trimmed.charAt(4);
					if ((ch0 == 102 || ch0 == 70) && (ch1 == 97 || ch1 == 65) && (ch2 == 108 || ch2 == 76)
							&& (ch3 == 115 || ch3 == 83) && (ch4 == 101 || ch4 == 69)) {
						return Boolean.FALSE;
					}
				}

				return null;
			}
		}
		return obj;
	}  
}
