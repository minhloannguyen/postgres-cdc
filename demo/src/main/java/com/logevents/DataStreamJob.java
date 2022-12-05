package com.logevents;

import org.apache.flink.shaded.guava30.com.google.common.base.CaseFormat;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.apache.flink.table.api.Expressions.*;

import java.util.Arrays;
import java.util.stream.Collectors;

public class DataStreamJob {
	private static final String SOURCE_TABLE = "app_log_events";
	private static final String FS_TABLE = "fs_table";
	private static final String TOPIC = "dbpostgres.public.app_log_events";
	private static final String BROKERS = "localhost:29092";
	private static final String GROUP_ID = "flink-streaming";
	private static final String OUTPUT_PATH = "data/sink/";

	private static Logger logger = LoggerFactory.getLogger(DataStreamJob.class);

	public static class TransformFunction extends ScalarFunction {
		public String eval(String data) {
			JsonObject obj = JsonParser.parseString(data).getAsJsonObject();
			JsonObject payload = JsonParser.parseString(obj.get("payload").getAsString()).getAsJsonObject();

			JsonObject result = new JsonObject();
			String[] schema = { "appVersion", "component", "session", "serialNumber" };
			for (String inputField : schema) {
				String outputField = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, inputField);
				result.addProperty(outputField, payload.get(inputField).getAsString());
			}

			return result.toString();
		}
	}

	public static void main(String[] args) throws Exception {
		logger.info("Start consuming {}", TOPIC);

		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// start a checkpoint every 1000 ms
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		final TableDescriptor sourceDescriptor = TableDescriptor
				.forConnector("kafka")
				.schema(
						Schema.newBuilder()
								.column("schema", DataTypes.STRING())
								.column("payload", DataTypes.STRING())
								.build())
				.format("json")
				.option("scan.startup.mode", "earliest-offset")
				// .option("scan.startup.mode", "group-offsets")
				.option("topic", TOPIC)
				.option("properties.bootstrap.servers", BROKERS)
				.option("properties.group.id", GROUP_ID)
				.option("json.ignore-parse-errors", "true")
				.build();

		tEnv.createTemporaryTable(SOURCE_TABLE, sourceDescriptor);
		// tEnv.executeSql("SELECT * from " + SOURCE_TABLE).print();
		Table table = tEnv.from(SOURCE_TABLE)
				.filter($("payload").isNotNull())
				.select($("payload").jsonQuery("$.after")).as("data")
				.select(call(TransformFunction.class, $("data"))).as("transformed_data");
		table.execute().print();

		final TableDescriptor fsDescriptor = TableDescriptor
				.forConnector("filesystem")
				.schema(Schema.newBuilder()
						.column("content", DataTypes.STRING())
						.build())
				.format("json")
				.option("sink.partition-commit.policy.kind", "success-file")
				.option("sink.partition-commit.delay", "1 d")
				.option("sink.rolling-policy.check-interval", "30 s")
				.option("sink.rolling-policy.file-size", "64 MB")
				.option("sink.rolling-policy.rollover-interval", "15 min")
				.option("path", OUTPUT_PATH)
				// .partitionedBy("year", "month", "day")
				.build();

		tEnv.createTemporaryTable(FS_TABLE, fsDescriptor);

		String query = "INSERT INTO " + FS_TABLE +
				" SELECT * " +
				// ", YEAR(`timestamp`) as `year`, " +
				// "MONTH(`timestamp`) AS `month`, " +
				// "DAYOFMONTH(`timestamp`) as `day` " +
				"from " + SOURCE_TABLE;

		// tEnv.executeSql(query);
	}
}
