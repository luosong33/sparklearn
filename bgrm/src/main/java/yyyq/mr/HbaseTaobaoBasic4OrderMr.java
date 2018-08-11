package yyyq.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import yyyq.util.CalculationUtil;
import yyyq.util.DateUtil;

import java.io.IOException;

public class HbaseTaobaoBasic4OrderMr {


    public static class MyMapper extends TableMapper<Text, Text> { // IntWritable

        @Override
        //输入的类型为：key：rowKey； value：一行数据的结果集Result
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String mapping_id = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("mapping_id")));
            String trade_id = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("trade_id")));
            String actual_fee = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("actual_fee")));
            String trade_status = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("trade_status")));
            String voucher_no = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("voucher_no")));

            context.write(new Text(mapping_id), new Text("1|" + actual_fee + "|" + trade_status + "|" +voucher_no));
        }
    }

    public static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum_order = 0;
            Double sum_order_money = Double.valueOf(0);
            int sum_no_pay_order = 0;
            Double sum_no_pay_order_money = Double.valueOf(0);
            for (Text val : values) {
                String[] split = val.toString().split("\\|");
                String i = split[0];
                String actual_fee = split[1];  //  订单金额
                String trade_status = split[2];  //  订单状态
                String voucher_no = split[3];  //  订单状态

                sum_order += Integer.parseInt(i);
                String add = CalculationUtil.add(String.valueOf(sum_order_money), actual_fee);
                sum_order_money = Double.valueOf(add);

                if ("TRADE_CLOSED".equals(trade_status) /*!"TRADE_FINISHED".equals(trade_status)  && !"WAIT_SELLER_SEND_GOODS".equals(trade_status)
                && !"SELLER_CONSIGNED_PART".equals(trade_status) && !"WAIT_BUYER_CONFIRM_GOODS".equals(trade_status)*/){
                    sum_no_pay_order += Integer.parseInt(i);
                    String add_ = CalculationUtil.add(String.valueOf(sum_no_pay_order_money), actual_fee);
                    sum_no_pay_order_money = Double.valueOf(add_);
                }

                Put put = new Put(Bytes.toBytes(key.toString()));
                put.add(Bytes.toBytes("c"), Bytes.toBytes("sum_order"), Bytes.toBytes(String.valueOf(sum_order)));
                put.add(Bytes.toBytes("c"), Bytes.toBytes("sum_order_money"), Bytes.toBytes(String.valueOf(sum_order_money)));
                put.add(Bytes.toBytes("c"), Bytes.toBytes("sum_no_pay_order"), Bytes.toBytes(String.valueOf(sum_no_pay_order)));
                put.add(Bytes.toBytes("c"), Bytes.toBytes("sum_no_pay_order_money"), Bytes.toBytes(String.valueOf(sum_no_pay_order_money)));
                put.add(Bytes.toBytes("c"), Bytes.toBytes("voucher_no"), Bytes.toBytes(String.valueOf(voucher_no)));
                put.add(Bytes.toBytes("c"), Bytes.toBytes("loadtime"), Bytes.toBytes(DateUtil.nowString()));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
            }

            /*Put put = new Put(Bytes.toBytes(entry.getKey()));
            put.add("c".getBytes(), "debtorId".getBytes(), Bytes.toBytes(client_no));
            context.write(null, put);*/
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径

        Job job = Job.getInstance(conf, "HbaseTaobaoBasic4OrderMr");
        job.setJarByClass(HbaseTaobaoBasic4OrderMr.class);//主类

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("mapping_id"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("trade_id"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("actual_fee"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("trade_status"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("voucher_no"));

        truncateTable("taobaobasic_tradedetails_summary");
        TableMapReduceUtil.initTableMapperJob("bops_client_taobaobasic_tradedetails", scan, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("taobaobasic_tradedetails_summary", MyReducer.class, job);   // contact_person_evaluate_result   test4
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void truncateTable(String tName){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.15.195");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

        TableName tableName = TableName.valueOf(tName);
        try {
            admin.disableTable(tableName); // 设置表状态为无效
            admin.truncateTable(tableName, true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                admin.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}