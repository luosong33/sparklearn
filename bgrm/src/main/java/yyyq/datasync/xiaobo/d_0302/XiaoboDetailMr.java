package yyyq.datasync.xiaobo.d_0302;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XiaoboDetailMr extends Configured {
    /**
     * MyMapper 继承 TableMapper
     * TableMapper<Text,IntWritable>
     * Text:输出的key类型，
     * IntWritable：输出的value类型
     */
    public static class MyMapper extends TableMapper<Text, Text> {

        @Override
        //输入的类型为：key：rowKey； value：一行数据的结果集Result
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String ID = Bytes.toString(row.get());

            String call_cnt_6m = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("call_cnt_6m")));
            String call_time_6m = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("call_time_6m")));
            String mobile_answer = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("mobile_answer")));

            String called_phone = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("called_phone")));

            if (mobile_answer != null && !"".equals(mobile_answer) && !"null".equals(mobile_answer) && mobile_answer.length() >= 11) {
                mobile_answer = phoneHandle(mobile_answer);
                context.write(new Text(mobile_answer), new Text("1|" + ID + "|" + call_cnt_6m + "|" + call_time_6m + "|" + mobile_answer));
            }
            if (called_phone != null && !"".equals(called_phone) && !"null".equals(called_phone) && called_phone.length() >= 11) {
                called_phone = phoneHandle(called_phone);
                context.write(new Text(called_phone), new Text("2|" + called_phone));
            }
        }
    }


    public static class MyReducer extends TableReducer<Text, Text, NullWritable> {  //  ImmutableBytesWritable
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<HashMap<String, String>> list1 = new ArrayList<>();
            boolean called_phone = false;
            for (Text val : values) {
                String s = val.toString();
                String[] split = s.split("\\|", -1);
                String flag = split[0];
                if ("1".equals(flag)) {
                    String ID = split[1];
                    String call_cnt_6m = split[2];
                    String call_time_6m = split[3];
                    String mobile_answer = split[4];
                    HashMap<String, String> hashMap = new HashMap<>();
                    hashMap.put("ID", ID);
                    hashMap.put("call_cnt_6m", call_cnt_6m);
                    hashMap.put("call_time_6m", call_time_6m);
                    hashMap.put("mobile_answer", mobile_answer);
                    list1.add(hashMap);
                } else if ("2".equals(flag)) {
                    called_phone = true;
                }
            }

            if (called_phone) {
                int i = 0;
                for (HashMap<String, String> map : list1) {
                    Put put = new Put(Bytes.toBytes(map.get("ID")));
                    put.add("c".getBytes(), "call_cnt_6m".getBytes(), Bytes.toBytes(map.get("call_cnt_6m")));
                    put.add("c".getBytes(), "call_time_6m".getBytes(), Bytes.toBytes(map.get("call_time_6m")));
                    put.add("c".getBytes(), "mobile_answer".getBytes(), Bytes.toBytes(map.get("mobile_answer")));
                    put.add("c".getBytes(), "sort".getBytes(), Bytes.toBytes(i++ + ""));
                    put.add("c".getBytes(), "count".getBytes(), Bytes.toBytes(list1.size() + ""));
                    context.write(NullWritable.get(), put);
                }
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径
        //创建job
        Job job = new Job(config, "OverdueCellDetailMr");//job
        job.setJarByClass(XiaoboDetailMr.class);//主类

        List<Scan> scans = new ArrayList<>();
        Scan scan1 = new Scan();
        scan1.setCaching(500);
        scan1.setCacheBlocks(false);
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("call_cnt_6m"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("call_time_6m"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("mobile_answer"));
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("capricorn_report_call_contact_detail"));  //  ods_cell_linker_test
        scan1.setStartRow(Bytes.toBytes(args[0] + "!"));
        scan1.setStopRow(Bytes.toBytes(args[0] + "~"));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setCaching(500);
        scan2.setCacheBlocks(false);
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("called_phone"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("xiaoboi_0302"));  //  rpt_xjb_data_test1
        scans.add(scan2);
        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);

        TableMapReduceUtil.initTableReducerJob("xiaoboi_0302_data", MyReducer.class, job);   // rpt_xjb_data_0211_test
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static String phoneHandle(String phone) {
        int size = phone.length();
        if (size > 3) {
            String substring = phone.substring(0, 3);
            if ("+86".equals(substring)) {
                phone = phone.substring(3, phone.length());
            }
        }
        //  提取纯数字
        String regEx = "[^0-9]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(phone);
        String trim = m.replaceAll("").trim();
        if (trim.length() > 11) {
            trim = trim.substring(0, 11);
        }
        return trim;
    }
}
