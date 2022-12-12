package com.logevents;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.logevents.DataStreamJob.FlatMapFunction;
import com.logevents.DataStreamJob.TransformMetadata;

public class DataStreamJobTest {
    static String payload = "";
    static String metadata = "";
    static FlatMapFunction flatMap = new FlatMapFunction();
    static TransformMetadata transform = new TransformMetadata();

    @BeforeAll
    public static void init() {
        try {
            Path payloadPath = Paths.get("src/test/java/com/data/input.txt");
            payload = new String(Files.readAllBytes(Paths.get(payloadPath.toAbsolutePath().toString())));

            Path metadataPath = Paths.get("src/test/java/com/data/metadata.txt");
            metadata = new String(Files.readAllBytes(Paths.get(metadataPath.toAbsolutePath().toString())));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    public static void cleanUp() {
        try {
            flatMap.close();
            transform.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testMain() throws Exception {
        StreamTableEnvironment tEnvClassMock = Mockito.mock(StreamTableEnvironment.class, Mockito.RETURNS_DEEP_STUBS);
        MockedStatic<StreamTableEnvironment> tableEnvMock = Mockito.mockStatic(StreamTableEnvironment.class);
        tableEnvMock.when(() -> StreamTableEnvironment.create(any(StreamExecutionEnvironment.class)))
                .thenReturn(tEnvClassMock);

        Table table = Mockito.mock(Table.class);
        Mockito.when(
                tEnvClassMock
                        .from(anyString())
                        .filter(any(Expression.class))
                        .select(any(Expression.class))
                        .flatMap(anyString()))
                .thenReturn(table);

        DataStreamJob.main(null);
    }

    @Test
    void testGetSchemaUnderscoreFields() {
        String[] underScoreColumns = DataStreamJob.getSchemaUnderscoreFields();
        String[] expectedOut = {
                "app_version",
                "component",
                "session",
                "platform",
                "serial_number",
                "user_id",
                "_metadata" };

        assertArrayEquals(expectedOut, underScoreColumns);
    }

    @Test
    void testTransformMetadata() {
        String id = "d24b8c60-c03c-11ec-b362-11b18ae4d379";
        int dataIndex = 20220420;
        String eventTime = "2022-04-20 07:00:00";
        Row expectedOut = Row.of(id, dataIndex, eventTime);

        JsonObject item = JsonParser.parseString(metadata).getAsJsonObject();
        Row actualOut = transform.doTransformation(item.get("lineage").getAsJsonArray().get(0));

        assertEquals(expectedOut, actualOut);
    }

    @Test
    public void testFlatMap() {
        String appVersion = "v93.7.7";
        String component = "ble";
        String session = "end_wifi";
        String platform = "android";
        String serialNumber = "dummy_98325d2332b66bd8099065b0af905615";
        String userId = "dummy_c17d46eab40f9434f026b2f45b488527";
        String _metadata = "{\"lineage\":[{\"id\":\"***b8c60-c03c-11ec-b362-11b18ae4d379\",\"time\":\"04/20/2022, 07:00:00\",\"phase\":\"ingestion\",\"target\":\"prd.ingestion.app-log.event\",\"timestamp\":1750412800038},{\"id\":\"@@@b8c60-c03c-11ec-b362-11b18ae4d379\",\"time\":\"04/20/2022, 07:00:00\",\"phase\":\"ingestion\",\"target\":\"prd.ingestion.app-log.event\",\"timestamp\":1650713800038}],\"content_hash\":\"6iqX1S2BVLq3C0iZanjRhg==\"}";
        Row expectedOut = Row.of(appVersion,
                component,
                session,
                platform,
                serialNumber,
                userId,
                _metadata);

        Row actualOut = flatMap.doMap(payload);

        assertEquals(expectedOut, actualOut);
    }

    @Test
    void testFlatMapTypeInformation() {
        TypeInformation<?>[] validSchemaType = new TypeInformation[7];
        Arrays.fill(validSchemaType, Types.STRING());

        TypeInformation<Row> out = flatMap.getResultType();
        assertEquals(Types.ROW(validSchemaType), out);
    }
}
