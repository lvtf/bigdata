package com.tbex.bd.utils;

import com.tbex.bd.model.HiveDataType;
import com.tbex.bd.model.OtherInfo;
import com.tbex.bd.model.MysqlDateType;
import com.tbex.bd.model.TableInfo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;

public class HiveShell {


    public static void mysql2Hive(String jdbcAddress, String dbName, String tbName, Boolean isPartition) throws Exception {
        String url = "jdbc:mysql://" + jdbcAddress + ":3306/" + dbName + "?tinyInt1isBit=false";
        Connection conn = DriverManager.getConnection(url, TableInfo.userName, TableInfo.passWord);
        String hiveTableName = dbName + OtherInfo._bd_ + tbName;

        DatabaseMetaData metaDb = conn.getMetaData();
        ResultSet colRet = metaDb.getColumns(null, "%", tbName, "%");

        //所有columnNameConcat的集合
        ArrayList<String> columnNameConcatList = new ArrayList<>();
        StringBuffer columnNameAndTypeSb = new StringBuffer();

        //where 条件
        String whereCondition = "";
        while (colRet.next()) {
            String columnName = colRet.getString("COLUMN_NAME");
            String columnType = colRet.getString("TYPE_NAME");
            String columnNameConcat;
            String columnTypeConcat;

            //所有字段和其匹配的类型
            if (columnType.equals(MysqlDateType.BIT.toString())) {
                columnTypeConcat = HiveDataType.BOOLEAN.toString();
            } else if (columnType.contains(MysqlDateType.TINYINT.toString())) {
                columnTypeConcat = HiveDataType.TINYINT.toString();
            } else if (columnType.contains(MysqlDateType.unsigned.getSmallInt_unsigned())) {
                columnTypeConcat = HiveDataType.SMALLINT.toString();
            } else if (columnType.equals(MysqlDateType.DOUBLE)) {
                columnTypeConcat = HiveDataType.DOUBLE.toString();
            } else if (columnType.equals(MysqlDateType.INT.toString())) {
                columnTypeConcat = HiveDataType.INT.toString();
            } else if (columnType.contains(MysqlDateType.BIGINT.toString())
                    || columnType.equals(MysqlDateType.unsigned.getInt_unsigned())) {
                columnTypeConcat = HiveDataType.BIGINT.toString();
            } else if (columnType.equals(MysqlDateType.DECIMAL.toString()) || columnType.equals(MysqlDateType.unsigned.getDecimal_unsigned())
                    || columnType.equals(MysqlDateType.CHAR.toString()) || columnType.equals(MysqlDateType.VARCHAR.toString())
                    || columnType.equals(MysqlDateType.DATETIME.toString()) || columnType.equals(MysqlDateType.DATE.toString())
                    || columnType.equals(MysqlDateType.TIME.toString()) || columnType.equals(MysqlDateType.TIMESTAMP.toString())) {
                columnTypeConcat = HiveDataType.STRING.toString();
            } else {
                throw new Exception("缺失这种类型：" + columnType);
            }

            //所有的字段（包含decimal、datetime等格式的）
            if (columnType.equals(MysqlDateType.BIT.toString()) || columnType.contains(MysqlDateType.TINYINT.toString())
                    || columnType.contains(MysqlDateType.SMALLINT.toString()) || columnType.contains(MysqlDateType.INT.toString())
                    || columnType.contains(MysqlDateType.BIGINT.toString()) || columnType.equals(MysqlDateType.CHAR.toString())
                    || columnType.equals(MysqlDateType.VARCHAR.toString()) || columnType.equals(MysqlDateType.DOUBLE.toString())) {
                columnNameConcat = columnName;
            } else if (columnType.contains(MysqlDateType.DECIMAL.toString())) {
                columnNameConcat = "concat(" + columnName + ",'') " + columnName;
            } else if (columnType.equals(MysqlDateType.TIME.toString()) || columnType.equals(MysqlDateType.DATETIME.toString())
                    || columnType.equals(MysqlDateType.TIMESTAMP.toString())) {
                columnNameConcat = "date_format(" + columnName + ",'%Y-%m-%d %H:%i:%s') " + columnName;
            } else {
                throw new Exception(columnType);
            }
            columnNameAndTypeSb.append(columnName + "=" + columnTypeConcat + ",");
            //将所有的字段放入list集合

            columnNameConcatList.add(columnNameConcat);


            //where判断条件
            if (columnName.contains("create") || columnName.contains("login")) {
                whereCondition = "date_format(" + columnName + ",'%Y-%m-%d') ";
            }

        }

        //--query
        String columnName = "";
        for (int i = 0; i < columnNameConcatList.size(); i++) {
            if (i <= columnNameConcatList.size() - 2) {
                columnName += columnNameConcatList.get(i) + ",";
            }
            if (i == columnNameConcatList.size() - 1) {
                columnName += columnNameConcatList.get(i);
            }
        }


        //--map-column-hive
        int index = columnNameAndTypeSb.lastIndexOf(",");

        String columnAndType = columnNameAndTypeSb.substring(0, index) + " " + columnNameAndTypeSb.substring(index + 1, columnNameAndTypeSb.length());
        String mapColumnHive = "--map-column-hive ".concat(columnAndType.toLowerCase()) + OtherInfo.nextLine.getEnter();


        String binBash = "#!/bin/bash  \n";

        String sqoopImport = "sqoop import " + OtherInfo.nextLine.getEnter();

        String jdbc = "--connect \"".concat(url + "\"") + OtherInfo.nextLine.getEnter() +
                "--username ".concat(TableInfo.userName) + OtherInfo.nextLine.getEnter() +
                "--password ".concat(TableInfo.passWord) + OtherInfo.nextLine.getEnter();

        //target目录
        String partitionDir = "$eight_days_ago2";
        //where条件
        String where = " where \\$CONDITIONS ";
        String targetDir =
                "--target-dir ".concat(TableInfo.hdfsLocationPath + hiveTableName) + OtherInfo.nextLine.getEnter();
        if (isPartition) {
            where = where + "and " + whereCondition + "<'${seven_days_ago}' and " + whereCondition + ">='${eight_days_ago}'";
            targetDir = "--target-dir ".concat(TableInfo.hdfsLocationPath + hiveTableName + "/" + partitionDir) + OtherInfo.nextLine.getEnter();
        }

        String query = "--query ".concat("\"select " + columnName + " from " + tbName + where + " \"") + OtherInfo.nextLine.getEnter();


        String deleteDir = "--delete-target-dir " + OtherInfo.nextLine.getEnter();
        String splitId = "--split-by ".concat(columnNameConcatList.get(0) + OtherInfo.nextLine.getEnter()) +
                "--null-string '\\\\N' " + OtherInfo.nextLine.getEnter() +
                "--null-non-string '\\\\N' " + OtherInfo.nextLine.getEnter() +
                "--as-parquetfile " + OtherInfo.nextLine.getEnter();

        String timeVariabl = "eight_days_ago=`date -d -8day +%Y-%m-%d`\n" +
                "seven_days_ago=`date -d -7day +%Y-%m-%d`\n" +
                "\n" +
                "year=${eight_days_ago:0:4}\n" +
                "month=${eight_days_ago:5:2}\n" +
                "day=${eight_days_ago:8:2}\n" +
                "eight_days_ago2=$year\"_\"$month\"_\"$day\n";

        String hdfsPath = "&& hadoop fs -rm -r -f " + TableInfo.hdfsLocationPath + hiveTableName + "/dt=$eight_days_ago " + OtherInfo.nextLine.getEnter() +
                "&& hadoop fs -mv " + TableInfo.hdfsLocationPath + hiveTableName + "/{$eight_days_ago2,dt=$eight_days_ago} " + OtherInfo.nextLine.getEnter() +
                "&& hadoop fs -chmod a+w " + TableInfo.hdfsLocationPath + hiveTableName + "/dt=$eight_days_ago \n\n" +
                "hive -e \"alter table " + OtherInfo.ods + "." + hiveTableName + " add if not exists partition(dt='$eight_days_ago');\"";

        StringBuffer sb = new StringBuffer();
        // 加分区
        if (isPartition) {
            sb.append(binBash)
                    .append(timeVariabl)
                    .append(sqoopImport)
                    .append(jdbc)
                    .append(targetDir)
                    .append(deleteDir)
                    .append(query)
                    .append(mapColumnHive)
                    .append(splitId)
                    .append(hdfsPath);
        } else {
            sb.append(binBash)
                    .append(sqoopImport)
                    .append(jdbc)
                    .append(targetDir)
                    .append(deleteDir)
                    .append(query)
                    .append(mapColumnHive)
                    .append(splitId);
        }

        FileWriter fw = new FileWriter("/home/tengfeilv/data_statistics_platform/" + dbName + "/" + hiveTableName + ".sh", false);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(sb.toString());
        bw.flush(); // 把缓存区内容压入文件
        bw.close(); // 最后记得关闭文件

    }
}