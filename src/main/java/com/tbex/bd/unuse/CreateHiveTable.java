/*
package com.tbex.bd.unuse;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class CreateHiveTable {


    public static void main(String[] args) throws SQLException, IOException {
        String partition = args[0];
        String jdbcAddress = args[1];
        String dbName = args[2];
        String tableName = args[3];


        String hiveDB = "ods";
        String userName = "root";
        String passWord = "root";
        String URL = "jdbc:mysql://" + jdbcAddress + ":3306/" + dbName + "?tinyInt1isBit=false";
        Connection con = DriverManager.getConnection(URL, userName, passWord);

        String sql = "select * from " + tableName + " limit 2";

        PreparedStatement stmt = con.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery("show full columns from " + tableName);
        ArrayList<String> filedsList = new ArrayList<>();
        ArrayList<String> all = new ArrayList<>();
        ArrayList<String> typesList = new ArrayList<>();
        ArrayList<String> commentsList = new ArrayList<>();
        HashMap<String, String> fieldAndCommentsMap = new HashMap<>();
        while (rs.next()) {
            String field = rs.getString("Field");
            filedsList.add(field);
            String type = rs.getString("Type");
            typesList.add(type);
            String comment = rs.getString("Comment");
            commentsList.add(comment);
            fieldAndCommentsMap.put(field, comment);
            if (type.contains("bigint")) {
                type = "bigint";
            } else if (type.contains("bit")) {
                type = "boolean";
            } else if (type.contains("tinyint")) {
                type = "tinyint";
            } else if (type.contains("smallint")) {
                type = "smallint";
            } else if (type.contains("int")) {
                if (type.endsWith("unsigned")) {
                    type = "bigint";
                } else {
                    type = "int";
                }
            } else if (type.contains("decimal")) {
                type = "string";
            } else if (type.contains("varchar") || type.contains("char") || type.contains("date")
                    || type.contains("datetime") || type.contains("timestamp")) {
                type = "string";
            }

            String key = field + "         " + type + "            " + "COMMENT '" + comment + "'";
            all.add(key);
        }
        String value = "";
        for (String key : all) {
            value += " " + key + ",\n";
        }
        String concat = value.substring(0, value.lastIndexOf(",")) + ")\n";
        String x = "create external table " + hiveDB + "." + dbName + "_bd_" + tableName + "(\n";


        StringBuffer buffer = new StringBuffer();


        if (partition.equals("1")) {
            partition = "partitioned by (dt string) row format delimited   fields terminated by '\\t'  STORED AS PARQUETFILE\n" +
                    "LOCATION '/data/hive/bigdata/ods/" + dbName + "_bd_" + tableName + "';";
        } else {
            partition = "row format delimited   fields terminated by '\\t'  STORED AS PARQUETFILE\n" +
                    "LOCATION '/data/hive/bigdata/ods/" + dbName + "_bd_" + tableName + "';";
        }
        String result = x + concat + partition;

        buffer.append(result);

        if (partition.equals("1")) {

            FileWriter fw = new FileWriter("/home/tengfeilv/data_statistics_platform/table_name/" + dbName + "_bd_" + tableName + "partition.sql", false);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(buffer.toString());
            fw.flush(); // 把缓存区内容压入文件
            bw.close(); // 最后记得关闭文件
        } else {
            FileWriter fw = new FileWriter("/home/tengfeilv/data_statistics_platform/table_name/" + dbName + "_bd_" + tableName + ".sql", false);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(buffer.toString());
            fw.flush(); // 把缓存区内容压入文件
            bw.close(); // 最后记得关闭文件
        }


        */
/*if (!file.exists()) {
            file.mkdir();
            FileWriter fw = new FileWriter("/home/tengfeilv/data_statistics_platform/table_name/" + dbName + "/" + dbName + "_bd_" + tableName + ".sql", false);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(buffer.toString());
            fw.flush(); // 把缓存区内容压入文件
            bw.close(); // 最后记得关闭文件
        } else {
            FileWriter fw = new FileWriter("/home/tengfeilv/data_statistics_platform/table_name/" + dbName + "/" + dbName + "_bd_" + tableName + ".sql", false);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(buffer.toString());
            fw.flush(); // 把缓存区内容压入文件
            bw.close(); // 最后记得关闭文件
*//*

    }


}

}
*/
