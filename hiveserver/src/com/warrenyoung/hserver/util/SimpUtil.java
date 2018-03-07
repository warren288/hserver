package com.warrenyoung.hserver.util;

import com.warrenyoung.hserver.bean.IString;

import java.util.List;

public class SimpUtil {

    public static <T extends IString> String combine(List<T> listStr, String combiner){
        StringBuilder sb = new StringBuilder();
        for (IString str : listStr){
            sb.append(str.getString());
            sb.append(combiner);
        }
        return sb.substring(0, sb.length()-combiner.length());
    }

    public static  String combineStrList(List<String> listStr, String combiner){
        StringBuilder sb = new StringBuilder();
        for (String str : listStr){
            sb.append(str);
            sb.append(combiner);
        }
        return sb.substring(0, sb.length()-combiner.length());
    }
}
