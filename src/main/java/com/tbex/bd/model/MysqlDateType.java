package com.tbex.bd.model;

public enum MysqlDateType {
    BIT,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    DECIMAL,
    DOUBLE,
    CHAR,
    VARCHAR,
    TIMESTAMP,
    DATETIME,
    TIME,
    DATE,
    UNSIGNED,


    unsigned("SMALLINT UNSIGNED", "INT UNSIGNED",
            "DECIMAL UNSIGNED", "BIGINT UNSIGNED ");

    private String smallInt_unsigned;
    private String int_unsigned;
    private String decimal_unsigned;
    private String bigint_unsigned;

    MysqlDateType(String smallInt_unsigned, String decimal_unsigned, String int_unsigned, String bigint_unsigned) {
        this.smallInt_unsigned = smallInt_unsigned;
        this.decimal_unsigned = decimal_unsigned;
        this.int_unsigned = int_unsigned;
        this.bigint_unsigned = bigint_unsigned;
    }


    MysqlDateType() {

    }


    public String getSmallInt_unsigned() {
        return smallInt_unsigned;
    }

    public String getInt_unsigned() {
        return int_unsigned;
    }

    public String getDecimal_unsigned() {
        return decimal_unsigned;
    }

    public String getBigint_unsigned() {
        return bigint_unsigned;
    }
}
