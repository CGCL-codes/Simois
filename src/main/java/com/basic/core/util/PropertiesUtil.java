package com.basic.core.util;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by windy on 2019/1/2.
 */
public class PropertiesUtil {
    public static final String fileName="/simois.properties";

    public static Properties pro;

    static{
        pro=new Properties();
        try {
            InputStream in = Object. class .getResourceAsStream( fileName );
            pro.load(in);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static String getProperties(String name){
        return pro.getProperty(name);
    }

    public static void setProperties(String name ,String value){
        pro.setProperty(name, value);
    }
}
