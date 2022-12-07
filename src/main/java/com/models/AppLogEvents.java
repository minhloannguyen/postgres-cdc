package com.models;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.guava30.com.google.common.base.CaseFormat;
import org.apache.flink.table.api.Types;

public enum AppLogEvents {
    APPVERSION("appVersion", Types.STRING()),
    COMPONENT("component", Types.STRING()),
    SESSION("session", Types.STRING()),
    PLATFORM("platform", Types.STRING()),
    SERIALNUMBER("serialNumber", Types.STRING()),
    USERID("userId", Types.STRING()),
    _METADATA("_metadata", Types.STRING());

    public final String field;
    public final TypeInformation<?> type;

    private AppLogEvents(String field, TypeInformation<?> type) {
        this.field = field;
        this.type = type;
    }

    public static String getUnderScoreName(String field) {
        for (AppLogEvents e : values()) {
            if (e.field.equals(field)) {
                return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, e.field);
            }
        }

        return null;
    }
}
