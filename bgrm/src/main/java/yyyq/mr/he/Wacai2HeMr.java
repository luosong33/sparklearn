package yyyq.mr.he;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import yyyq.mr.Ebaoquan2EmayMr;
import yyyq.mr.EbaoquanMr;
import yyyq.util.DateUtil;
import yyyq.util.IdWorker;
import yyyq.util.SnowflakeIdGenerator;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Wacai2HeMr {

    public static class MyMapper extends TableMapper<Text, Text> {

        @Override
        //输入的类型为：key：rowKey； value：一行数据的结果集Result
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String ID = Bytes.toString(row.get());
            String name = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("name")));
            String valueDate = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("valueDate")));
            String detail_debtor = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_debtor")));
            String identityNo = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("identityNo")));
            String detail_guarantee = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_guarantee")));
            String durationDays = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("durationDays")));
            String detail_useing = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_useing")));
            String annualRate = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("annualRate")));
            String categoryDisplayName = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("categoryDisplayName")));
            String telephone = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("telephone")));
            String maturityDate = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("maturityDate")));
            String loanAmt = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("loanAmt")));
            String detail_source = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_source")));
            String contractNo = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("contractNo")));
            String age = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("age")));
            String detail_start = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_start")));
            String claimNumber = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("claimNumber")));
            String sex = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("sex")));
            String detail_method = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_method")));
            String GMT_CREATED = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("GMT_CREATED")));
            String GMT_MODIFIED = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("GMT_MODIFIED")));

            context.write(new Text(ID), new Text(
//                    ID + "|" +
                            name + "|" +
                            valueDate + "|" +
                            detail_debtor + "|" +
                            identityNo + "|" +
                            detail_guarantee + "|" +
                            durationDays + "|" +
                            detail_useing + "|" +
                            annualRate + "|" +
                            categoryDisplayName + "|" +
                            telephone + "|" +
                            maturityDate + "|" +
                            loanAmt + "|" +
                            detail_source + "|" +
                            contractNo + "|" +
                            age + "|" +
                            detail_start + "|" +
                            claimNumber + "|" +
                            sex + "|" +
                            detail_method + "|" +
                            GMT_CREATED + "|" +
                            GMT_MODIFIED));
        }
    }


    public static class MyReducer extends TableReducer<Text, Text, NullWritable> {  //  ImmutableBytesWritable
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                String rowKey = key.toString();
//                SnowflakeIdGenerator idWorker = new SnowflakeIdGenerator(0, 0);
//                rowKey = idWorker.nextId() + "";
                String[] split = val.toString().split("\\|");
                String name = split[0];
                int length = name.length();
                if (length % 2 == 0){
                    String s1 = name.substring(0, length / 2);
                    String s2 = name.substring(length / 2, length);
                    if (s1.equals(s2)){
                        name = s1;
                    }
                }
                String valueDate = split[1];
                String detail_debtor = split[2];
                String identityNo = split[3];
                String detail_guarantee = split[4];
                String durationDays = split[5];
                String detail_useing = split[6];
                String annualRate = split[7];
                String categoryDisplayName = split[8];
                String telephone = split[9];
                String maturityDate = split[10];
                String loanAmt = split[11];
                String detail_source = split[12];
                String contractNo = split[13];
                String age = split[14];
                String detail_start = split[15];
                String claimNumber = split[16];
                String sex = split[17];
                String detail_method = split[18];
                String GMT_CREATED = split[19];
                String GMT_MODIFIED = split[20];

                Put put = new Put(Bytes.toBytes(rowKey+"_wacai"));
                put.add("c".getBytes(), "name".getBytes(), Bytes.toBytes(name));
                put.add("c".getBytes(), "valueDate".getBytes(), Bytes.toBytes(valueDate));
                put.add("c".getBytes(), "detail_debtor".getBytes(), Bytes.toBytes(detail_debtor));
                put.add("c".getBytes(), "identityNo".getBytes(), Bytes.toBytes(identityNo));
                put.add("c".getBytes(), "detail_guarantee".getBytes(), Bytes.toBytes(detail_guarantee));
                put.add("c".getBytes(), "durationDays".getBytes(), Bytes.toBytes(durationDays));
                put.add("c".getBytes(), "detail_useing".getBytes(), Bytes.toBytes(detail_useing));
                put.add("c".getBytes(), "annualRate".getBytes(), Bytes.toBytes(annualRate));
                put.add("c".getBytes(), "categoryDisplayName".getBytes(), Bytes.toBytes(categoryDisplayName));
                put.add("c".getBytes(), "telephone".getBytes(), Bytes.toBytes(telephone));
                put.add("c".getBytes(), "maturityDate".getBytes(), Bytes.toBytes(maturityDate));
                put.add("c".getBytes(), "loanAmt".getBytes(), Bytes.toBytes(loanAmt));
                put.add("c".getBytes(), "detail_source".getBytes(), Bytes.toBytes(detail_source));
                put.add("c".getBytes(), "contractNo".getBytes(), Bytes.toBytes(contractNo));
                put.add("c".getBytes(), "age".getBytes(), Bytes.toBytes(age));
                put.add("c".getBytes(), "detail_start".getBytes(), Bytes.toBytes(detail_start));
                put.add("c".getBytes(), "claimNumber".getBytes(), Bytes.toBytes(claimNumber));
                put.add("c".getBytes(), "sex".getBytes(), Bytes.toBytes(sex));
                put.add("c".getBytes(), "detail_method".getBytes(), Bytes.toBytes(detail_method));
                put.add("c".getBytes(), "GMT_CREATED".getBytes(), Bytes.toBytes(GMT_CREATED));
                put.add("c".getBytes(), "GMT_MODIFIED".getBytes(), Bytes.toBytes(GMT_MODIFIED));
                put.add("c".getBytes(), "project".getBytes(), Bytes.toBytes("挖财"));
                put.add("c".getBytes(), "corporate_name".getBytes(), Bytes.toBytes("杭州挖财互联网金融服务有限公司"));
                put.add("c".getBytes(), "product_name".getBytes(), Bytes.toBytes("挖财"));
                put.add("c".getBytes(), "contract_attribute".getBytes(), Bytes.toBytes("贷款"));
                put.add("c".getBytes(), "source".getBytes(), Bytes.toBytes("挖财"));
                context.write(NullWritable.get(), put);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径
        //创建job
        Job job = new Job(config, "Wacai2HeMr");//job
        job.setJarByClass(Ebaoquan2EmayMr.class);//主类

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("valueDate"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("detail_debtor"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("identityNo"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("detail_guarantee"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("durationDays"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("detail_useing"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("annualRate"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("categoryDisplayName"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("telephone"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("maturityDate"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("loanAmt"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("detail_source"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("contractNo"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("age"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("detail_start"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("claimNumber"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("sex"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("detail_method"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("GMT_CREATED"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("GMT_MODIFIED"));

        TableMapReduceUtil.initTableMapperJob("wacai_result_data_res", scan, Wacai2HeMr.MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("ewe", Wacai2HeMr.MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
