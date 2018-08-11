package yyyq.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class CallDurationStatistics extends Configured {
	public static class ReaderHbaseMapper extends TableMapper<Text, Text> {
		public static final byte[] C = "c".getBytes();
		public static final byte[] peerNumberb = "peer_number".getBytes();
		public static final byte[] mobileb = "mobile".getBytes();
		public static final byte[] durationb = "duration".getBytes();

		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws InterruptedException, IOException {
			if (null != value.getValue(C, mobileb) && null != value.getValue(C, peerNumberb)
					&& null != value.getValue(C, durationb)) {
				String mobile = new String(value.getValue(C, mobileb));
				String duration = new String(value.getValue(C, durationb));
				String peerNumber = new String(value.getValue(C, peerNumberb));
				if (StringUtils.isNumeric(peerNumber)) {
					context.write(new Text(mobile), new Text(duration + "|" + peerNumber));
				}
			}
		}
	}

	public static class EvaluateContactPersonReduce extends TableReducer<Text, Text, ImmutableBytesWritable> {

		@SuppressWarnings("deprecation")
		public void reduce(Text key, Iterable<Text> values, Context context) {
			String mobile = key.toString();
			Map<String, Long> durationMap = new HashMap<String, Long>();
			for (Text val : values) {
				String duration = val.toString().split("\\|", 2)[0];
				String peerNumber = val.toString().split("\\|", 2)[1];
				if (durationMap.containsKey(peerNumber)) {
					Long temp = durationMap.get(peerNumber);
					durationMap.put(peerNumber, temp + Long.valueOf(duration));
				} else {
					durationMap.put(peerNumber, Long.valueOf(duration));
				}
			}
			List<Entry<String, Long>> list = new ArrayList<Entry<String, Long>>(durationMap.entrySet());
			Collections.sort(list, new Comparator<Entry<String, Long>>() {
				// 降序排序
				@Override
				public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
					return o2.getValue().compareTo(o1.getValue());
				}
			});
			for (int i=0;i<list.size();i++) {  
				Entry<String, Long> mapping=list.get(i);
				String peerNumber=mapping.getKey();
				Put put = new Put(Bytes.toBytes(mobile.toString()));
			    put.add("c".getBytes(), "mobile".getBytes(), Bytes.toBytes(mobile));
			    put.add("c".getBytes(), "phoneNum".getBytes(), Bytes.toBytes(peerNumber));
				if(i<10){
				    put.add("c".getBytes(), "tag".getBytes(), Bytes.toBytes("优"));
				    put.add("c".getBytes(), "order".getBytes(), Bytes.toBytes("1"));
				    try {
						context.write(null, put);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}else if(i>=10&&i<20){
					put.add("c".getBytes(), "tag".getBytes(), Bytes.toBytes("中"));
				    put.add("c".getBytes(), "order".getBytes(), Bytes.toBytes("2"));
				    try {
						context.write(null, put);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}else{
					put.add("c".getBytes(), "tag".getBytes(), Bytes.toBytes("差"));
				    put.add("c".getBytes(), "order".getBytes(), Bytes.toBytes("3"));
				    try {
						context.write(null, put);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
	        }  
		}
	}

	public static void main(String[] args) throws Exception {
		String tableName = "moxie_basic_call_details";
		String targetTableName = "contact_person_evaluate_result";
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "192.168.15.195");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Job job = Job.getInstance(conf, "callDutationStatistics");
		job.setJarByClass(CallDurationStatistics.class);

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("mobile"));
		scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("peer_number"));
		scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("duration"));
		TableMapReduceUtil.initTableMapperJob(tableName, // input
				scan, // Scan instance to control CF and attribute selection
				ReaderHbaseMapper.class, // mapper
				Text.class, // mapper output key
				Text.class, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(targetTableName, // output table
				EvaluateContactPersonReduce.class, // reducer class
				job);

		job.waitForCompletion(true);
		System.exit(job.isSuccessful() ? 0 : 1);
	}

}
