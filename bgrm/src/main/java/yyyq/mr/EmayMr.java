package yyyq.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import yyyq.util.DateUtil;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EmayMr {

    public static class MyMapper extends TableMapper<Text, Text> {
        @Override
        //输入的类型为：key：rowKey； value：一行数据的结果集Result
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            //获取一行数据中的colf：col
            String ID = Bytes.toString(row.get());

            String cert_no = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("cert_no")));
            String client_no = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("client_no")));
            String emr007_loanlendersamount = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("emr007_loanlendersamount")));
            String emr007_loanlenderstime = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("emr007_loanlenderstime")));
            String emr007_p_type = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("emr007_p_type")));
            String emr007_platformcode = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("emr007_platformcode")));
            String loadtime = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("loadtime")));
            String voucher_no = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("voucher_no")));

            if ("2017".equals(emr007_loanlenderstime.substring(0,4))) {
                context.write(new Text(cert_no), new Text(ID + "|" + cert_no + "|" + client_no + "|" + emr007_loanlendersamount + "|" + emr007_loanlenderstime + "|"
                        + emr007_p_type + "|" + emr007_platformcode + "|" + loadtime + "|" + voucher_no));
            }
        }
    }


    public static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String s = val.toString();
                String ID = s.split("\\|")[0];
                String cert_no = s.split("\\|")[1];
                String client_no = s.split("\\|")[2];
                String emr007_loanlendersamount = s.split("\\|")[3];
                String emr007_loanlenderstime = s.split("\\|")[4];
                String emr007_p_type = s.split("\\|")[5];
                String emr007_platformcode = s.split("\\|")[6];
                String loadtime = s.split("\\|")[7];
                String voucher_no = s.split("\\|")[8];

                Put put = new Put(Bytes.toBytes(ID));
                put.add("c".getBytes(), "cert_no".getBytes(), Bytes.toBytes(cert_no));
                put.add("c".getBytes(), "client_no".getBytes(), Bytes.toBytes(client_no));
                put.add("c".getBytes(), "emr007_loanlendersamount".getBytes(), Bytes.toBytes(emr007_loanlendersamount));
                put.add("c".getBytes(), "emr007_loanlenderstime".getBytes(), Bytes.toBytes(emr007_loanlenderstime));
                put.add("c".getBytes(), "emr007_p_type".getBytes(), Bytes.toBytes(emr007_p_type));
                put.add("c".getBytes(), "emr007_platformcode".getBytes(), Bytes.toBytes(emr007_platformcode));
                put.add("c".getBytes(), "loadtime".getBytes(), Bytes.toBytes(loadtime));
                put.add("c".getBytes(), "voucher_no".getBytes(), Bytes.toBytes(voucher_no));
                context.write(null, put);
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("df.default.name", "hdfs://master:8020/");  //  设置hdfs的默认路径

        Job job = Job.getInstance(conf, "EmayMr");
        job.setJarByClass(EmayMr.class);//主类
        //创建scan
        Scan scan = new Scan();
        //可以指定查询某一列
//        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("ROW"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("cert_no"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("client_no"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("emr007_loanlendersamount"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("emr007_loanlenderstime"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("emr007_p_type"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("emr007_platformcode"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("loadtime"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("voucher_no"));
        TableMapReduceUtil.initTableMapperJob("emay_emr007", scan, EmayMr.MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("emay_emr007_2017", EmayMr.MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
