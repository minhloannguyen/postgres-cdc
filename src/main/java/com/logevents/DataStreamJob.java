package com.logevents;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static org.apache.flink.table.api.Expressions.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import com.models.AppLogEvents;

public class DataStreamJob {

	private static final String SOURCE_TABLE = "app_log_events";
	private static final String FS_TABLE = "fs_table";
	private static final String TOPIC = "dbpostgres.public.app_log_events";
	private static final String BROKERS = "localhost:29092";
	private static final String GROUP_ID = "flink-streaming";
	private static final String OUTPUT_PATH = "data/sink/";

	// values() is a static method of Enum, which always returns the values in same
	// order => important for mapping dtypes
	private static final AppLogEvents[] schema = AppLogEvents.values();

	private static Logger logger = LoggerFactory.getLogger(DataStreamJob.class);

	public static class TransformFunction extends ScalarFunction {
		public String eval(String data) {
			JsonObject obj = JsonParser.parseString(data).getAsJsonObject();
			JsonObject payload = JsonParser.parseString(obj.get("payload")
					.getAsString())
					.getAsJsonObject();
			JsonArray metadata = payload.get("_metadata")
					.getAsJsonObject()
					.get("lineage")
					.getAsJsonArray();

			JsonObject transformedObj = new JsonObject();
			ArrayList<JsonObject> result = new ArrayList<JsonObject>();

			// Convert field name from camel to underscore
			// for extendable schema
			for (AppLogEvents e : schema) {
				String outputField = AppLogEvents.getUnderScoreName(e.field);
				transformedObj.addProperty(outputField, payload.get(e.field).getAsString());
			}

			for (JsonElement item : metadata) {
				JsonObject lineage = JsonParser.parseString(item.toString()).getAsJsonObject();
				long timestamp = lineage.get("timestamp").getAsLong();
				String dateIndex = new SimpleDateFormat("yyyyMMdd")
						.format(new Date(timestamp));
				String eventTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
						.format(new Date(timestamp));

				transformedObj.addProperty("id", lineage.get("id").getAsString());
				transformedObj.addProperty("date_index", Integer.parseInt(dateIndex));
				transformedObj.addProperty("event_time", eventTime);

				result.add(transformedObj);
			}
			// for (JsonElement item : metadata) {
			// JsonObject lineage =
			// JsonParser.parseString(item.toString()).getAsJsonObject();
			// long timestamp = lineage.get("timestamp").getAsLong();
			// String dateIndex = new SimpleDateFormat("yyyyMMdd")
			// .format(new Date(timestamp));
			// String eventTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
			// .format(new Date(timestamp));

			// transformedObj.addProperty("id", lineage.get("id").getAsString());
			// transformedObj.addProperty("date_index", Integer.parseInt(dateIndex));
			// transformedObj.addProperty("event_time", eventTime);

			// result.add(transformedObj);
			// }

			return result.toString();
		}
	}

	public static class FlatMapFunction extends TableFunction<Row> {

		public void eval(String data) {
			JsonObject obj = JsonParser.parseString(data).getAsJsonObject();
			JsonObject payload = JsonParser.parseString(obj.get("payload")
					.getAsString())
					.getAsJsonObject();
			Row row = Row.withPositions(schema.length);

			// for extendable schema
			for (int i = 0; i < schema.length; i++) {
				String value = "";
				try {
					value = payload.get(schema[i].field).getAsString();

				} catch (UnsupportedOperationException ex) {
					// Catch _metadata (cannot getAsString)
					value = payload.get(schema[i].field).getAsJsonObject().toString();
				}
				row.setField(i, value);
			}
			collect(row);
		}

		@Override
		public TypeInformation<Row> getResultType() {
			int schemaLength = schema.length;
			TypeInformation<?>[] dtypes = new TypeInformation[schemaLength];
			for (int i = 0; i < schemaLength; i++) {
				dtypes[i] = schema[i].type;
			}
			return Types.ROW(dtypes);
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

		// Initialize source table
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
		TableFunction func = new FlatMapFunction();
		tEnv.registerFunction("func", func);

		Table table = tEnv.from(SOURCE_TABLE)
				.filter($("payload").isNotNull())
				.select($("payload").jsonQuery("$.after")).as("data")
				// .select(call(TransformFunction.class, $("data"))).as("transformed_data");
				.flatMap(call("func", $("data"))).as("app_version",
						"component",
						"session",
						"platform",
						"serial_number",
						"user_id",
						"_metadata");
		table.execute().print();

		// Create sink table
		final TableDescriptor fsDescriptor = TableDescriptor
				.forConnector("filesystem")
				.schema(Schema.newBuilder()
						.column("transformed_data", DataTypes.STRING())
						.build())
				.format("json")
				.option("sink.partition-commit.policy.kind", "success-file")
				.option("sink.partition-commit.delay", "1 d")
				.option("sink.rolling-policy.check-interval", "30 s")
				.option("sink.rolling-policy.file-size", "64 MB")
				.option("sink.rolling-policy.rollover-interval", "31 s")
				// .option("sink.rolling-policy.rollover-interval", "15 min")
				.option("path", OUTPUT_PATH)
				// .partitionedBy("year", "month", "day")
				.build();

		tEnv.createTemporaryTable(FS_TABLE, fsDescriptor);
		table.insertInto(FS_TABLE).execute();
	}
}
