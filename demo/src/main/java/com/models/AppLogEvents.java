package com.models;

public enum AppLogEvents {
    APPVERSION("appVersion"),
    COMPONENT("component"),
    SESSION("session");

    public final String[] schema;

    private AppLogEvents(){
        this.schema = new String[]{"a", "b", "c"};
    }

    // public static QueueName valueOfQueueName(String queueName) {
    //     for (QueueName e : values()) {
    //         if (e.queueName.equals(queueName)) {
    //             return e;
    //         }
    //     }

    //     return null;
    // }
}
