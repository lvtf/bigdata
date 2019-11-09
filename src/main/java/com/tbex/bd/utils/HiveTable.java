package com.tbex.bd.utils;

import com.tbex.bd.model.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;

public class HiveTable {
    public static void createHiveTable(String jdbcAddress, String dbName, String tbName, Boolean isPartition) throws Exception {

        String url = "jdbc:mysql://" + jdbcAddress + ":3306/" + dbName + "?tinyInt1isBit=false";
        Connection conn = DriverManager.getConnection(url, TableInfo.userName, TableInfo.passWord);
        String hiveTableName = dbName + OtherInfo._bd_ + tbName;

        String sql = "select * from " + tbName + " limit 2";
        PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery("show full columns from " + tbName);


        ArrayList<String> filedsTypesCommentsList = new ArrayList<>();

        while (rs.next()) {
            String field = rs.getString("Field");
            String type = rs.getString("Type").toUpperCase();
            String comment = rs.getString("Comment");

            if (type.contains(MysqlDateType.BIT.toString())) {
                type = HiveDataType.BOOLEAN.toString();
            } else if (type.contains(MysqlDateType.TINYINT.toString())) {
                type = HiveDataType.TINYINT.toString();
            } else if (type.contains(MysqlDateType.SMALLINT.toString())) {
                type = HiveDataType.SMALLINT.toString();
            } else if (type.contains(MysqlDateType.BIGINT.toString())) {
                type = HiveDataType.BIGINT.toString();
            } else if (type.contains(MysqlDateType.INT.toString())) {
                if (type.contains(MysqlDateType.UNSIGNED.toString())) {
                    type = HiveDataType.BIGINT.toString();
                } else {
                    type = HiveDataType.INT.toString();
                }

            } else if (type.contains(MysqlDateType.DOUBLE.toString())) {
                type = HiveDataType.DOUBLE.toString();
            } else if (type.contains(MysqlDateType.DECIMAL.toString()) || type.contains(MysqlDateType.VARCHAR.toString())
                    || type.contains(MysqlDateType.VARCHAR.toString()) || type.contains(MysqlDateType.DATETIME.toString())
                    || type.contains(MysqlDateType.TIMESTAMP.toString()) || type.contains(MysqlDateType.DATE.toString())) {
                type = HiveDataType.STRING.toString();
            } else {
                throw new Exception("缺失的类型" + type);
            }
            String key = field + "         " + type.toLowerCase() + "            " + "COMMENT '" + comment + "'";
            filedsTypesCommentsList.add(key);
        }
        String dateType = "";
        for (int i = 0; i < filedsTypesCommentsList.size(); i++) {
            if (i <= filedsTypesCommentsList.size() - 2) {
                dateType += filedsTypesCommentsList.get(i) + ",\n";
            }
            if (i == filedsTypesCommentsList.size() - 1) {
                dateType += filedsTypesCommentsList.get(i) + ")\n";
            }
        }


        //建表语句的容器
        StringBuffer DDLSb = new StringBuffer();
        String tableFirstLine = TableInfo.createExternalTable + OtherInfo.ods + "." + hiveTableName + "(\n";
        String tableLastLine = TableInfo.splitCharater + TableInfo.fileType +
                "LOCATION '".concat(TableInfo.hdfsLocationPath + hiveTableName + "';");

        System.out.println(isPartition);
        //加分区
        if (isPartition) {
            tableLastLine = "partitioned by(dt string)" + TableInfo.splitCharater + TableInfo.fileType +
                    "LOCATION '".concat(TableInfo.hdfsLocationPath + hiveTableName + "';");
        }
        String result = tableFirstLine + dateType + tableLastLine;


        DDLSb.append(result);
        FileWriter fw = new FileWriter("/home/tengfeilv/data_statistics_platform/table_name/" + hiveTableName + ".sql", false);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(DDLSb.toString());
        bw.flush(); // 把缓存区内容压入文件
        bw.close(); // 最后记得关闭文件

    }
}
