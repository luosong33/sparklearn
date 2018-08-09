package yyyq.datasync.jingshuang;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/* 与user表撞到的装了盈盈易贷的用户  查询他们的通讯录 mr方式 */
public class YyydInfoMr extends Configured {
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
            String clientNo = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("clientNo")));
            String linker = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("linker")));
            String linkPhone = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("linkPhone")));

            String client_no = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("client_no")));

            if (clientNo != null && !"".equals(clientNo) && !"null".equals(clientNo)) {
                context.write(new Text(clientNo), new Text("1|" + clientNo + "|" + linker + "|" + linkPhone));
            }
            if (!"".equals(client_no) && client_no != null && !"null".equals(client_no)) {
                context.write(new Text(client_no), new Text("2|" + client_no));
            }
        }
    }

    /**
     * MyReducer 继承 TableReducer
     * TableReducer<Text,IntWritable>
     * Text:输入的key类型，
     * IntWritable：输入的value类型，
     * ImmutableBytesWritable：输出类型，表示rowkey的类型
     */
    public static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                for (Text val : values) {  //  通讯录和user表
                    String[] split = val.toString().split("\\|");
                    String flag = split[0]; //  表标记
                    if ("2".equals(flag)) {  //  通讯录撞到user

                        for (Text val_ : values) {
                            String[] split_ = val_.toString().split("\\|");
                            String flag_ = split_[0];  //  表标记
                            if ("1".equals(flag_)) {  //
                                String clientNo = split_[1];  //  联系人对应客户号
                                String linker = split_[2];  //  联系人名称
                                String linkPhone = split_[3];  //  联系人号码
                                Put put = new Put(Bytes.toBytes(clientNo + "_" + linkPhone));
                                put.add("c".getBytes(), "clientNo".getBytes(), Bytes.toBytes(clientNo));
                                put.add("c".getBytes(), "linker".getBytes(), Bytes.toBytes(linker));
                                put.add("c".getBytes(), "linkPhone".getBytes(), Bytes.toBytes(linkPhone));
                                context.write(null, put);
                            }
                        }
                        continue;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径
        //创建job
        Job job = new Job(config, "YyydInfoMr");//job
        job.setJarByClass(YyydInfoMr.class);//主类

        List<Scan> scans = new ArrayList<>();
        Scan scan1 = new Scan();
        scan1.setCaching(500);
        scan1.setCacheBlocks(false);
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("clientNo"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("linker"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("linkPhone"));
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("ods_cell_linker"));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setCaching(500);
        scan2.setCacheBlocks(false);
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("client_no"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("yyyd_info_user"));
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("yyyd_info_result", MyReducer.class, job);   // contact_person_evaluate_result   test4
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}