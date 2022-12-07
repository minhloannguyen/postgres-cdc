package com.models;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;

public class AppLogEventsTest {

    private static AppLogEvents[] schema = new AppLogEvents[] {};

    @BeforeAll
    public static void init() {
        schema = AppLogEvents.values();
    }

    @DisplayName("Test Event Schema Values")
    @Test
    public void testSchemaValues() {
        String[] validSchema = {
                "appVersion",
                "component",
                "session",
                "platform",
                "serialNumber",
                "userId",
                "_metadata"
        };
        for (int i = 0; i < validSchema.length; i++) {
            assertEquals(validSchema[i], schema[i].field);
        }
    }

    @DisplayName("Test Event Field Type")
    @Test
    public void testTypeValues() {
        TypeInformation<?>[] validSchemaType = {
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
        };
        for (int i = 0; i < validSchemaType.length; i++) {
            assertEquals(validSchemaType[i], schema[i].type);
        }
    }

    @DisplayName("Test getUnderScoreName")
    @Test
    public void testGetUnderScoreName() {
        String[] validName = {
                "app_version",
                "component",
                "session",
                "platform",
                "serial_number",
                "user_id",
                "_metadata"
        };
        for (int i = 0; i < validName.length; i++) {
            String outputName = AppLogEvents.getUnderScoreName(schema[i].field);
            assertEquals(validName[i], outputName);
        }
    }
}
