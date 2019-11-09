package com.tbex.bd.model;

public class TableInfo {

    /*     public final static String userName = "root";
         public final static String passWord = "58mysql456";*/
/*    public final static String userName = "appuser";
    public final static String passWord = "976xmX5kKuRNjnIy";*/
    public static final String userName = "bigdata";
    public static final String passWord = "CiNMeJjodReGcewf";

    public static String hdfsLocationPath = "/data/hive/bigdata/ods/";

    public static String createExternalTable = "create external table ";

    public static String splitCharater = "row format delimited fields terminated by '\\t' ";
    public static String fileType = "stored as parquetfile ";





   /*String p = "partitioned by (dt string) row format delimited   fields terminated by '\\t'  STORED AS PARQUETFILE\n" +
                "LOCATION '" + Info.locationPpath + dbName + OtherInfo._bd_ + tbName + "';";*/

}
