package com.tbex.bd.model;

public enum OtherInfo {
    thirty_one_days_ago,
    thirty_days_ago,
    eight_days_ago,
    seven_days_ago,
    three_days_ago,
    two_days_ago,

    ods,
    _bd_,
    nextLine(" \\\n");
    private String enter;

    OtherInfo(String enter) {
        this.enter = enter;
    }


    OtherInfo() {
    }

    public String getEnter() {
        return enter;
    }


}
