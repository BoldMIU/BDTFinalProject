package bdt;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import bdt.SparkApp;

public class SparkApp {
	private static final Collection<String> TOPICS = Arrays.asList("expense");
	private static final String TABLE_NAME = "expense_db";
	private static final String CF_DEFAULT = "row_key";
	private static final String CF_DATA = "student_data";
	private static final String CF_EXPENSE = "expense";

	public static void main(String[] args) throws InterruptedException,
			ClassNotFoundException, SQLException, IOException {

		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.set("spark.serializer",
						"org.apache.spark.serializer.KryoSerializer")
				.setAppName("Apache SPARK Application");
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "DEFAULT_GROUP_ID");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT)
					.setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_DATA));
			table.addFamily(new HColumnDescriptor(CF_EXPENSE));

			System.out.println("Creating table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			System.out.println("Table created.... ");
		}

		try (JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(10))) {

			JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
					.createDirectStream(ssc, LocationStrategies
							.PreferConsistent(), ConsumerStrategies
							.<String, String> Subscribe(TOPICS, kafkaParams));

			stream.foreachRDD(rdd -> {
				List<StudentExpense> student = rdd.map(ConsumerRecord::value)
						.map(StrToDtoParser::parse) 
						.filter(f -> Integer.parseInt(f.getAge()) >= 20)
						.filter(f -> Integer.parseInt(f.getMonthlyExpnses()) <= 300)
						.collect();
				if (student.isEmpty()) {
					System.out.println("Data empty!.... ");
					return;
				}
				for (StudentExpense s : student) {
					HTable dsTable = new HTable(config, TABLE_NAME);
					Put row = new Put(Bytes.toBytes(s.getId()));

					row.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("id"),
							Bytes.toBytes(s.getId()));
					row.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("gender"),
							Bytes.toBytes(s.getGender()));
					row.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("age"),
							Bytes.toBytes(s.getAge()));
					row.add(Bytes.toBytes(CF_DATA),
							Bytes.toBytes("study_year"),
							Bytes.toBytes(s.getStudyYear()));
					row.add(Bytes.toBytes(CF_DATA), Bytes.toBytes("living"),
							Bytes.toBytes(s.getLiving()));
					row.add(Bytes.toBytes(CF_DATA),
							Bytes.toBytes("scholarship"),
							Bytes.toBytes(s.getScholarship()));
					row.add(Bytes.toBytes(CF_DATA),
							Bytes.toBytes("partTimeJob"),
							Bytes.toBytes(s.getPartTimeJob()));
					row.add(Bytes.toBytes(CF_DATA),
							Bytes.toBytes("transporting"),
							Bytes.toBytes(s.getTransporting()));
					row.add(Bytes.toBytes(CF_EXPENSE),
							Bytes.toBytes("monthly_subscription"),
							Bytes.toBytes(s.getMonthlySubsciption()));
					row.add(Bytes.toBytes(CF_EXPENSE),
							Bytes.toBytes("monthly_expense"),
							Bytes.toBytes(s.getMonthlyExpnses()));

					dsTable.put(row);
					System.out.println("Data inserted!.... ");
				}
			rdd.coalesce(1).saveAsTextFile("/home/cloudera/workspace/final.project/output");
			});
			ssc.start();
			ssc.awaitTermination();
		}
	}

	public static class StrToDtoParser {
		public static StudentExpense parse(String s) {
			final String[] data = s.trim().split(",");

			StudentExpense student = new StudentExpense();
			student.setId(data[0].trim());
			student.setGender(data[1].trim());
			student.setAge(data[2].trim());
			student.setStudyYear(data[3].trim());
			student.setLiving(data[4].trim());
			student.setScholarship(data[5].trim());
			student.setPartTimeJob(data[6].trim());
			student.setTransporting(data[7].trim());
			student.setMonthlySubsciption(data[12].trim());
			student.setMonthlyExpnses(data[13].trim());

			return student;
		}
	}
}
