package com.tbex.bd.model;

public class TableInfo {

    public static final String userName = "root";
    public static final String passWord = "root";

    public static String hdfsLocationPath = "/data/hive/bigdata/ods/";

    public static String createExternalTable = "create external table ";

    public static String splitCharater = "row format delimited fields terminated by '\\t' ";
    public static String fileType = "stored as parquetfile ";





   /*String p = "partitioned by (dt string) row format delimited   fields terminated by '\\t'  STORED AS PARQUETFILE\n" +
                "LOCATION '" + Info.locationPpath + dbName + OtherInfo._bd_ + tbName + "';";*/

}
