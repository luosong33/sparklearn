package yyyq.datasync.jingshuang;

import jpype.AESDecrypt;
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
import java.util.Map;

/* 与user表撞到的装了盈盈易贷的用户  查询他们在通讯录的被叫及其相关  mr方式 */
public class YyydInfoReportMr extends Configured {
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
            String mobile_local = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("mobile_local")));
            String mobile_answer = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("mobile_answer")));
            String group_name = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("group_name")));
            String company_name = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("company_name")));

            String mobile = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("mobile")));
            try {
                mobile = new AESDecrypt().getDecrypt(mobile);
            } catch (Exception e) {
                System.out.println("解析错误");
                e.printStackTrace();
            }

            if (mobile_local != null && !"".equals(mobile_local) && !"null".equals(mobile_local)
                    && mobile_answer != null && !"".equals(mobile_answer) && !"null".equals(mobile_answer)) {
                context.write(new Text(mobile_answer), new Text("1|" + mobile_local + "|" + mobile_answer + "|" + group_name + "|" + company_name));
            }
            if (!"".equals(mobile) && mobile != null && !"null".equals(mobile)) {
                context.write(new Text(mobile), new Text("2|" + mobile));
            }
        }
    }

    public static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("----------------values----------------");
            System.out.println("----------------values----------------");
            System.out.println("----------------values----------------");
            System.out.println("----------------values----------------");
            System.out.println("----------------values----------------");
            System.out.println("----------------values----------------");
            ArrayList<HashMap<String, String>> list1 = new ArrayList<>();
            ArrayList<HashMap<String, String>> list2 = new ArrayList<>();
            for (Text val : values) {  //  通讯记录和user表
                String[] split = val.toString().split("\\|");
                String flag = split[0]; //  表标记
                if ("1".equals(flag)) {
                    String mobile_local = split[1];
                    String mobile_answer = split[2];
                    String group_name = split[3];
                    String company_name = split[4];
                    HashMap<String, String> hashMap = new HashMap<>();
                    hashMap.put("mobile_local", mobile_local);
                    hashMap.put("mobile_answer", mobile_answer);
                    hashMap.put("group_name", group_name);
                    hashMap.put("company_name", company_name);
                    list1.add(hashMap);
                } else if ("2".equals(flag)) {
                    String mobile = split[1];
                    HashMap<String, String> hashMap = new HashMap<>();
                    hashMap.put("mobile", mobile);
                    list2.add(hashMap);
                }
            }

            if (list2.size() > 0) {
                System.out.println("----------------"+list2.size()+"----------------");
                System.out.println("----------------"+list2.size()+"----------------");
                System.out.println("----------------"+list2.size()+"----------------");
                System.out.println("----------------"+list2.size()+"----------------");
                System.out.println("----------------"+list2.size()+"----------------");
                System.out.println("----------------"+list2.size()+"----------------");
                for (HashMap<String, String> map : list1) {
                    Put put = new Put(Bytes.toBytes(map.get("mobile_answer")+"_"+map.get("mobile_local")));
                    put.add("c".getBytes(), "mobile_local".getBytes(), Bytes.toBytes(map.get("mobile_local")));
                    put.add("c".getBytes(), "group_name".getBytes(), Bytes.toBytes(map.get("group_name")));
                    put.add("c".getBytes(), "company_name".getBytes(), Bytes.toBytes(map.get("company_name")));
                    context.write(null, put);
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
        Job job = new Job(config, "YyydInfoReportMr");//job
        job.setJarByClass(YyydInfoReportMr.class);//主类

        List<Scan> scans = new ArrayList<>();
        Scan scan1 = new Scan();
        scan1.setCaching(500);
        scan1.setCacheBlocks(false);
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("mobile_local"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("mobile_answer"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("group_name"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("company_name"));
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("capricorn_report_call_contact_detail"));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setCaching(500);
        scan2.setCacheBlocks(false);
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("mobile"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("yyyd_info_user"));
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("yyyd_info_0103_result", MyReducer.class, job);   // contact_person_evaluate_result   test4
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}