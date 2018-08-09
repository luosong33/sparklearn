package yyyq.mr;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import yyyq.util.RowKeyUtil;
import yyyq.util.SnowflakeIdGenerator;

import java.io.IOException;
import java.util.Iterator;


public class Zhima2HbaseMr {
    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String line = value.toString();
            String line = new String(value.getBytes(), "GBK");
            JSONObject jsob = JSON.parseObject(line);
            String exSerial = String.valueOf(jsob.get("exSerial"));
            String bizNo = String.valueOf(jsob.get("bizNo"));
            String isMatched = String.valueOf(jsob.get("isMatched"));
            String success = String.valueOf(jsob.get("success"));

            JSONArray jarr = jsob.getJSONArray("details");
            if (jarr != null) {
                int i = 0;
                for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
                    SnowflakeIdGenerator idWorker = new SnowflakeIdGenerator(0, 0);
                    long id = idWorker.nextId();
                    StringBuffer put = new StringBuffer(exSerial + "_" + id + i);
                    i++;
                    System.out.println(put.toString());
                    put.append("|1");

                    JSONObject job = (JSONObject) iterator.next();
                    String bizCode = job.getString("bizCode");
                    String code = job.getString("code");
                    String level = job.getString("level");
                    String refreshTime = job.getString("refreshTime");
                    String settlement = job.getString("settlement");
                    String type = job.getString("type");
                    JSONArray extendInfoJarr = job.getJSONArray("extendInfo");
                    for (Iterator iterator_ = extendInfoJarr.iterator(); iterator_.hasNext(); ) {
                        JSONObject infoJob = (JSONObject) iterator_.next();
                        String key_ = infoJob.getString("key");
                        String value_ = infoJob.getString("value");
                        put.append("|" + key_ + ":" + value_);
                    }
                    put.append("|" + "bizCode" + ":" + bizCode);
                    put.append("|" + "code" + ":" + code);
                    put.append("|" + "level" + ":" + level);
                    put.append("|" + "refreshTime" + ":" + refreshTime);
                    put.append("|" + "settlement" + ":" + settlement);
                    put.append("|" + "type" + ":" + type);

                    put.append("|" + "bizNo" + ":" + bizNo);
                    put.append("|" + "isMatched" + ":" + isMatched);
                    put.append("|" + "success" + ":" + success);
//                    context.write(new Text(exSerial + "_" + idWorker), new Text(put.toString()));
                    context.write(key, new Text(put.toString()));
                }
            } else {
                String idWorker = RowKeyUtil.getIdWorker(5);
                StringBuffer put = new StringBuffer(exSerial + "_" + idWorker);
                put.append("|2");
                put.append("|" + "bizNo" + ":" + bizNo);
                put.append("|" + "isMatched" + ":" + isMatched);
                put.append("|" + "success" + ":" + success);
                context.write(key, new Text(put.toString()));
            }
        }

    }

    public static class MyReducer extends TableReducer<LongWritable, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                String[] split = val.toString().split("\\|");
                if ("1".equals(split[1])) {
                    Put put = new Put(Bytes.toBytes(split[0]));
                    put.add("c".getBytes(), "event_max_amt_code".getBytes(), Bytes.toBytes(split[2].split(":")[1]));
                    put.add("c".getBytes(), "id".getBytes(), Bytes.toBytes(split[3].split(":")[1]));
                    put.add("c".getBytes(), "event_end_time_desc".getBytes(), Bytes.toBytes(split[4].split(":")[1]));
                    put.add("c".getBytes(), "bizCode".getBytes(), Bytes.toBytes(split[5].split(":")[1]));
                    put.add("c".getBytes(), "code".getBytes(), Bytes.toBytes(split[6].split(":")[1]));
                    put.add("c".getBytes(), "level".getBytes(), Bytes.toBytes(split[7].split(":")[1]));
                    put.add("c".getBytes(), "refreshTime".getBytes(), Bytes.toBytes(split[8].split(":")[1]));
                    put.add("c".getBytes(), "settlement".getBytes(), Bytes.toBytes(split[9].split(":")[1]));
                    put.add("c".getBytes(), "type".getBytes(), Bytes.toBytes(split[10].split(":")[1]));
                    put.add("c".getBytes(), "bizNo".getBytes(), Bytes.toBytes(split[11].split(":")[1]));
                    put.add("c".getBytes(), "isMatched".getBytes(), Bytes.toBytes(split[12].split(":")[1]));
                    put.add("c".getBytes(), "success".getBytes(), Bytes.toBytes(split[13].split(":")[1]));
                    context.write(null, put);
                } else {
                    Put put = new Put(Bytes.toBytes(split[0]));
                    put.add("c".getBytes(), "bizNo".getBytes(), Bytes.toBytes(split[2].split(":")[1]));
                    put.add("c".getBytes(), "isMatched".getBytes(), Bytes.toBytes(split[3].split(":")[1]));
                    put.add("c".getBytes(), "success".getBytes(), Bytes.toBytes(split[4].split(":")[1]));
                    context.write(null, put);
                }
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("df.default.name", "hdfs://192.168.15.195:8020/");//设置hdfs的默认路径
        //创建job
        Job job = Job.getInstance(config, "Zhima2HbaseMr");
        job.setJarByClass(Zhima2HbaseMr.class);
        job.setNumReduceTasks(1);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob("zhima_black_basic", Zhima2HbaseMr.MyReducer.class, job);

        String filepath = "";
//        filepath = "hdfs://192.168.15.195:8020/bigdata/datasync/zhima_black/20171027/*";
        filepath = "file:///tmp/nfs/cdsp/zhimaBlack/20171027/500120171027551836.txt";    //  linux本地
//        filepath = "D:\\tmp\\500120171027551836.txt";  //  win本地
        if (args.length > 0) {
            filepath = args[0];
        }
        Path inPath = new Path(filepath);
        FileInputFormat.addInputPath(job, inPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}