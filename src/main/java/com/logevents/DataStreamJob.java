package com.logevents;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
import java.util.Arrays;
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
	private static final int schemaLength = schema.length;

	private static Logger logger = LoggerFactory.getLogger(DataStreamJob.class);

	@FunctionHint(output = @DataTypeHint("ROW<id STRING, date_index INT, event_time STRING>"))
	public static class TransformMetadata extends TableFunction<Row> {

		public void eval(String metadata) {
			JsonObject obj = JsonParser.parseString(metadata).getAsJsonObject();
			JsonArray lineage = obj.get("lineage").getAsJsonArray();

			for (JsonElement item : lineage) {
				JsonObject objItem = JsonParser.parseString(item.toString()).getAsJsonObject();

				String id = objItem.get("id").getAsString();
				long timestamp = objItem.get("timestamp").getAsLong();
				String dateIndex = new SimpleDateFormat("yyyyMMdd")
						.format(new Date(timestamp));
				String eventTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
						.format(new Date(timestamp));

				collect(Row.of(id, Integer.parseInt(dateIndex), eventTime));
			}
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
				String field = schema[i].field;
				try {
					value = payload.get(field).getAsString();

				} catch (UnsupportedOperationException uoe) {
					// Catch _metadata (cannot getAsString)
					value = payload.get(field).getAsJsonObject().toString();

				} catch (Exception ex) {
					// in case missing fields from data source
					logger.error("Invalid field in input schema " + field, ex);
				}
				row.setField(i, value);
			}
			collect(row);
		}

		@Override
		public TypeInformation<Row> getResultType() {
			TypeInformation<?>[] dtypes = new TypeInformation[schemaLength];
			for (int i = 0; i < schemaLength; i++) {
				dtypes[i] = schema[i].type;
			}
			return Types.ROW(dtypes);
		}
	}

	private static String[] getSchemaUnderscoreFields() {
		String[] allCols = new String[schemaLength];
		for (int i = 0; i < schemaLength; i++)
			allCols[i] = AppLogEvents.getUnderScoreName(schema[i].field);

		return allCols;
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

		String[] allCols = getSchemaUnderscoreFields();
		String[] colsExceptFirst = Arrays.copyOfRange(allCols, 1, schemaLength);

		// Transform
		Table table = tEnv.from(SOURCE_TABLE)
				.filter($("payload").isNotNull())
				.select($("payload").jsonQuery("$.after")).as("data")
				.flatMap(call("func", $("data")))
				.as(allCols[0], colsExceptFirst)
				.joinLateral(call(TransformMetadata.class, $("_metadata")))
				.dropColumns($("_metadata"));
		table.execute().print();

		// // Create sink table
		// final TableDescriptor fsDescriptor = TableDescriptor
		// .forConnector("filesystem")
		// .schema(Schema.newBuilder()
		// .fromResolvedSchema(table.getResolvedSchema())
		// .build())
		// .format("json")
		// .option("sink.partition-commit.policy.kind", "success-file")
		// .option("sink.partition-commit.delay", "1 d")
		// .option("sink.rolling-policy.check-interval", "30 s")
		// .option("sink.rolling-policy.file-size", "64 MB")
		// .option("sink.rolling-policy.rollover-interval", "15 min")
		// .option("path", OUTPUT_PATH)
		// .build();
		// tEnv.createTemporaryTable(FS_TABLE, fsDescriptor);

		// table.insertInto(FS_TABLE).execute();
	}
}
