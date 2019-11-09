package com.tbex.bd.unuse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

public class SqoopMethodAll {


    /*public static void main(String[] args) throws Exception {



        String jdbcAddress = args[0];
        String dbName = args[1];
        String tableName = args[2];

        String userName = "bigdata";
        String passWord = "CiNMeJjodReGcewf";

        String dburl = "jdbc:mysql://" + jdbcAddress + ":3306/" + dbName + "?tinyInt1isBit=false";




        Connection con = DriverManager.getConnection(dburl, userName, passWord);
        DatabaseMetaData metaDb = con.getMetaData();

        ResultSet colRet = metaDb.getColumns(null, "%", tableName, "%");

        //字段和类型所对应的集合
        HashMap<String, String> columnNameAndTypeMap = new HashMap<>();

        //所有columnNameConcat的集合
        ArrayList<String> columnNameConcatList = new ArrayList<>();

        StringBuffer sb = new StringBuffer();
        String whereCondition = "";
        while (colRet.next()) {
            String columnName = colRet.getString("COLUMN_NAME");
            String columnType = colRet.getString("TYPE_NAME");
            String columnNameConcat;
            String columnTypeConcat;

            //所有字段和其匹配的类型
            if (columnType.contains("BIT")) {
                columnTypeConcat = "boolean";
            } else if (columnType.contains("TINYINT")) {
                columnTypeConcat = "tinyint";
            } else if (columnType.contains("SMALLINT UNSIGNED")) {
                columnTypeConcat = "smallint";
            } else if (columnType.contains("DOUBLE")) {
                columnTypeConcat = "double";
            } else if (columnType.equals("INT")) {
                columnTypeConcat = "int";
            } else if (columnType.equals("INT UNSIGNED") || columnType.equals("BIGINT") || columnType.equals("BIGINT UNSIGNED")) {
                columnTypeConcat = "bigint";
            } else if (columnType.equals("DECIMAL") || columnType.equals("DECIMAL UNSIGNED") || columnType.equals("CHAR")
                    || columnType.equals("VARCHAR") || columnType.equals("DATETIME") || columnType.equals("DATE")
                    || columnType.equals("TIME") || columnType.equals("TIMESTAMP")) {
                columnTypeConcat = "string";
            } else {
                throw new Exception("缺失这种类型：" + columnType);
            }

            //所有的字段（包含decimal、datetime等格式的）
            if (columnType.equals("BIT") || columnType.contains("TINYINT") || columnType.equals("INT")
                    || columnType.equals("INT UNSIGNED") || columnType.equals("BIGINT") || columnType.equals("BIGINT UNSIGNED")
                    || columnType.equals("CHAR") || columnType.equals("VARCHAR") || columnType.equals("DOUBLE") || columnType.contains("SMALLINT UNSIGNED")) {
                columnNameConcat = columnName;
            } else if (columnType.equals("DECIMAL") || columnType.equals("DECIMAL UNSIGNED")) {
                columnNameConcat = "concat(" + columnName + ",'') " + columnName;
            } else if (columnType.equals("TIME") || columnType.equals("DATETIME") || columnType.equals("TIMESTAMP")) {
                columnNameConcat = "date_format(" + columnName + ",'%Y-%m-%d %H:%i:%s') " + columnName;
            } else if (columnType.equals("DATE")) {
                columnNameConcat = "date_format(" + columnName + ",'%Y-%m-%d') " + columnName;
            } else {
                throw new Exception(columnType);
            }

            sb.append(columnName + "=" + columnTypeConcat + ",");

            //将所有的字段放入list集合
            columnNameConcatList.add(columnNameConcat);

            //将所有的字段和类型放入map
            columnNameAndTypeMap.put(columnName, columnTypeConcat);

            //where判断条件
            if (columnName.contains("create") || columnName.contains("login")) {
                whereCondition = "date_format(" + columnName + ",'%Y-%m-%d') ";
            }
            if (!columnName.contains("create") && columnName.contains("registe")) {
                whereCondition = "date_format(" + columnName + ",'%Y-%m-%d') ";
            }

        }

        //--query
        String columnName = "";
        for (String key : columnNameConcatList) {
            columnName += key + ",";
        }
        int index = columnName.lastIndexOf(",");
        //去掉最后一个逗号
        columnName = columnName.substring(0, index) + " " + columnName.substring(index + 1, columnName.length());
        String query = "--query \"select " + columnName + "from " + tableName + " where \\$CONDITIONS \" \\\n";


        //--map-column-hive
        String columnAndType = sb.toString();
        int i = columnAndType.lastIndexOf(",");
        columnAndType = columnAndType.substring(0, i) + " " + columnAndType.substring(i + 1, columnAndType.length());
        String mapColumnHive = "--map-column-hive " + columnAndType + "\\\n";


        String time = "#!/bin/bash\n\n";

        String jdbc = "sqoop import \\\n" +
                "--connect \"jdbc:mysql://" + jdbcAddress + ":3306/" + dbName + "?tinyInt1isBit=false&characterEncoding=utf8\" \\\n" +
                "--username " + userName + " \\\n" +
                "--password " + passWord + " \\\n";

        String targetDirAndDeleteTargetDir = "--target-dir /data/hive/bigdata/ods/" + dbName + "_bd_" + tableName + " \\\n" +
                "--delete-target-dir \\\n";

        String splitId = "--split-by id \\\n" +
                "--null-string '\\\\N' \\\n" +
                "--null-non-string '\\\\N' \\\n" +
                "--as-parquetfile \n";


        StringBuffer buffer = new StringBuffer();
        buffer.append(time).append(jdbc).append(targetDirAndDeleteTargetDir).append(query).append(mapColumnHive).append(splitId);
        File file = new File("/home/tengfeilv/data_statistics_platform/" + dbName + "/");
        if(!file.exists()){
            file.mkdir();
            FileWriter fw = new FileWriter("/home/tengfeilv/data_statistics_platform/" + dbName + "/" + dbName + "_bd_" + tableName + ".sh", false);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(buffer.toString());
            fw.flush(); // 把缓存区内容压入文件
            bw.close(); // 最后记得关闭文件
        }else {
            FileWriter fw = new FileWriter("/home/tengfeilv/data_statistics_platform/" + dbName + "/" + dbName + "_bd_" + tableName + ".sh", false);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(buffer.toString());
            fw.flush(); // 把缓存区内容压入文件
            bw.close(); // 最后记得关闭文件

        }


    }*/
}