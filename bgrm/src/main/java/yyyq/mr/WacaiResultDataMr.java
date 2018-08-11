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

public class WacaiResultDataMr {

    public static class MyMapper extends TableMapper<Text, Text> {
        @Override
        //输入的类型为：key：rowKey； value：一行数据的结果集Result
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            //获取一行数据中的colf：col
//            String ID = Bytes.toInt(row.get())+"";
            String name = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("name")));
            String valueDate = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("valueDate")));
            String detail_debtor = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_debtor")));
            String identityNo = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("identityNo")));
            String detail_guarantee = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_guarantee")));
            String durationDays = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("durationDays")));  //  借款时长
            String detail_useing = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_useing")));
            String annualRate = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("annualRate")));
            String TransferContractNo = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("TransferContractNo")));
            String categoryDisplayName = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("categoryDisplayName")));
            String telephone = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("telephone")));
            String maturityDate = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("maturityDate")));  //  借款到期日
            String loanAmt = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("loanAmt")));
            String detail_source = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_source")));
            String contractNo = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("contractNo")));
            String age = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("age")));
            String detail_start = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_start")));  //  借款开始日
            String claimNumber = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("claimNumber")));
            String sex = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("sex")));
            String detail_method = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("detail_method")));
            String GMT_CREATED = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("GMT_CREATED")));
            String GMT_MODIFIED = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("GMT_MODIFIED")));

            try {
                if (detail_start == null && maturityDate != null && durationDays != null) {
                    detail_start = completionDate(null, maturityDate, durationDays);
                } else if (maturityDate == null && detail_start != null && durationDays != null) {
                    maturityDate = completionDate(detail_start, null, durationDays);
                } else if (durationDays == null && detail_start != null && maturityDate != null) {
                    durationDays = completionDate(detail_start, maturityDate, null);
                }

                if (categoryDisplayName == null) {
                    categoryDisplayName = detail_useing;
                }
            } catch (Exception e) {
//                    e.printStackTrace();
//                    System.out.println("========detail_start======="+detail_start+"===============");
//                    System.out.println("========maturityDate======="+maturityDate+"===============");
//                    System.out.println("========durationDays======="+durationDays+"===============");
            }

            context.write(new Text(TransferContractNo), new Text(name + "|" +
                    valueDate + "|" +
                    detail_debtor + "|" +
                    identityNo + "|" +
                    detail_guarantee + "|" +
                    durationDays + "|" +
                    detail_useing + "|" +
                    annualRate + "|" +
                    TransferContractNo + "|" +
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
            for (Text val : values) {
                String[] split = val.toString().split("\\|");
                String name = split[0];
                String valueDate = split[1];
                String detail_debtor = split[2];
                String identityNo = split[3];
                String detail_guarantee = split[4];
                String durationDays = split[5];
                String detail_useing = split[6];
                String annualRate = split[7];
                String TransferContractNo = split[8];
                String categoryDisplayName = split[9];
                String telephone = split[10];
                String maturityDate = split[11];
                String loanAmt = split[12];
                String detail_source = split[13];
                String contractNo = split[14];
                String age = split[15];
                String detail_start = split[16];
                String claimNumber = split[17];
                String sex = split[18];
                String detail_method = split[19];
                String GMT_CREATED = split[20];
                String GMT_MODIFIED = split[21];

                Put put = new Put(Bytes.toBytes(TransferContractNo));
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
                context.write(null, put);
            }

        }
    }

    public static String completionDate(String startDate, String endDate, String durationDate) throws ParseException {
        String startDfs = "yyyy-MM-dd";
        String endDfs = "yyyy年MM月dd日";
        int i = 0;
        if (durationDate != null) {
            i = Integer.parseInt(durationDate.substring(0, durationDate.length() - 1));
        }

        if (startDate == null) {
            String substring = endDate.substring(4, 5);
            if ("-".equals(substring)) {
                startDate = DateUtil.getFrontAfterDate(endDate, -i, startDfs, startDfs);
            } else {
                startDate = DateUtil.getFrontAfterDate(endDate, -i, endDfs, startDfs);
            }
            return startDate;
        } else if (endDate == null) {
            String substring = startDate.substring(4, 5);
            if ("年".equals(substring)) {
                endDate = DateUtil.getFrontAfterDate(startDate, i, endDfs, endDfs);
            } else {
                endDate = DateUtil.getFrontAfterDate(startDate, i, startDfs, endDfs);
            }
            return endDate;
        } else {
            String diffs = "";
            String start = startDate.substring(4, 5);
            String end = endDate.substring(4, 5);
            if ("年".equals(start) && "年".equals(end)) {
                diffs = DateUtil.getDateDiff(startDate, endDate, endDfs, endDfs);
            } else if ("年".equals(start) && "-".equals(end)) {
                diffs = DateUtil.getDateDiff(startDate, endDate, endDfs, startDfs);
            } else if ("-".equals(start) && "-".equals(end)) {
                diffs = DateUtil.getDateDiff(startDate, endDate, startDfs, startDfs);
            } else if ("-".equals(start) && "年".equals(end)) {
                diffs = DateUtil.getDateDiff(startDate, endDate, startDfs, endDfs);
            }
            String diff = diffs.split("\\|")[3];  //  天
            return diff;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径

        Job job = Job.getInstance(conf, "WacaiResultDataMr");
        job.setJarByClass(WacaiResultDataMr.class);//主类
        //创建scan
        Scan scan = new Scan();
        /*scan.setStartRow(Bytes.toBytes(args[0] + "!"));
        scan.setStopRow(Bytes.toBytes(args[0] + "~"));*/
        //可以指定查询某一列
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("valueDate"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("detail_debtor"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("identityNo"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("detail_guarantee"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("durationDays"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("detail_useing"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("annualRate"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("TransferContractNo"));
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
        TableMapReduceUtil.initTableMapperJob("wacai_result_data", scan, WacaiResultDataMr.MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("wacai_result_data_res", WacaiResultDataMr.MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
