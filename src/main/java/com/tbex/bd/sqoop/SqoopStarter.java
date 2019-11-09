package com.tbex.bd.sqoop;


import com.tbex.bd.utils.HiveShell;
import com.tbex.bd.utils.HiveTable;

public class SqoopStarter {
    public static void main(String[] args) throws Exception {

        String jdbcAddress = args[0];
        String dbName = args[1];
        String tbName = args[2];
        Boolean isPartition = new Boolean(args[3]);

      /*  String jdbcAddress = "192.168.112.36";
        String dbName = "mortgage";
        String tbName = "tb_borrow_record";
        Boolean isPartition = new Boolean("false");*/

        HiveTable.createHiveTable(jdbcAddress, dbName, tbName, isPartition);
        HiveShell.mysql2Hive(jdbcAddress, dbName, tbName, isPartition);


    }
}
