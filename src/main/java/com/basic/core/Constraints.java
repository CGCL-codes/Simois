package com.basic.core;


import com.basic.core.util.PropertiesUtil;

/**
 * locate com.basic.storm
 * Created by windy on 2019/1/2.
 */
public class Constraints {
    public static int Threshold_r = Integer.valueOf(PropertiesUtil.getProperties("Threshold_r"));//
    public static int Threshold_l = Integer.valueOf(PropertiesUtil.getProperties("Threshold_l"));//
    public static double Threshold_p = Double.valueOf(PropertiesUtil.getProperties("Threshold_p"));//Attenuation probability

    public static final String SPLITTER_BOLT_ID = "splitter-bolt";
    public static final String COIN_BOLT_ID= "coin-bolt";
    public static final String PREDICTOR_BOLT_ID= "predictor-bolt";

    public static final String coinFileds="coin";
    public static final String wordFileds="key";
    public static final String relFileds="relation";
    public static final String tsFileds="timestamp";
    public static final String valueFileds="value";
    public static final String baseCountFileds="basecount";
    public static final String hotRFileds="hotR";
    public static final String nohotRFileds="nohotR";
    public static final String hotRFileds="hotS";
    public static final String nohotRFileds="nohotS";
    public static final String coinCountFileds="coincount";
    public static final String typeFileds="type";
}
