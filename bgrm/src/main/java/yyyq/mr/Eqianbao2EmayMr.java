package yyyq.mr;

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
import java.util.List;

public class Eqianbao2EmayMr extends Configured {
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

            String CERT_NO = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("CERT_NO")));
            String contract_attribute = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("contract_attribute")));

            String cert_no = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("cert_no")));

            if (CERT_NO != null && !"".equals(CERT_NO) && !"null".equals(CERT_NO)
                    && "贷款".equals(contract_attribute)) {
                context.write(new Text(CERT_NO), new Text("1"));
            }
            if (cert_no != null && !"".equals(cert_no) && !"null".equals(cert_no))
                context.write(new Text(cert_no), new Text("2"));
        }
    }


    public static class MyReducer extends TableReducer<Text, Text, NullWritable> {  //  ImmutableBytesWritable
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Put put = new Put(Bytes.toBytes(key.toString()));
            int i = 0;
            int j = 0;
            for (Text val : values) {
                String flag = val.toString();
                if ("1".equals(flag)) {
                    i++;
                } else {
                    j++;
                }
            }
            put.add("c".getBytes(), "count_eqianbao".getBytes(), Bytes.toBytes(i + ""));
            put.add("c".getBytes(), "count_emay".getBytes(), Bytes.toBytes(j + ""));
            context.write(NullWritable.get(), put);
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径
        //创建job
        Job job = new Job(config, "OverdueCellDetailMr");//job
        job.setJarByClass(Eqianbao2EmayMr.class);//主类

        List<Scan> scans = new ArrayList<>();
        Scan scan1 = new Scan();
        scan1.setCaching(500);
        scan1.setCacheBlocks(false);
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CERT_NO"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("contract_attribute"));
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("e_qianbao_data_24h_formal"));  //  e_qianbao_data_1m_test e_qianbao_data_24h_formal
        scan1.setStartRow(Bytes.toBytes(args[0] + "!"));
        scan1.setStopRow(Bytes.toBytes(args[0] + "~"));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setCaching(500);
        scan2.setCacheBlocks(false);
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("cert_no"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("emay_emr007_2017"));
        scans.add(scan2);
        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);

        TableMapReduceUtil.initTableReducerJob("e_qianbao_24h_emay_2017", MyReducer.class, job);   // contact_person_evaluate_result   test4
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
