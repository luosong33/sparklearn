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
import yyyq.util.DateUtil;
import yyyq.util.IdWorker;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Ebaoquan2HeMr {

    public static class MyMapper extends TableMapper<Text, Text> {

        @Override
        //输入的类型为：key：rowKey； value：一行数据的结果集Result
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String ID = Bytes.toString(row.get());
            String CERT_NO = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("CERT_NO")));

            String project = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("source")));
            String time = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("time")));
            String GMT_CREATED = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("GMT_CREATED")));
            String GMT_MODIFIED = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("GMT_MODIFIED")));
            String corporate_name = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("corporate_name")));
            String contract_attribute = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("contract_attribute")));
            String product_name = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("product_name")));

            String REAL_NAME = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("REAL_NAME")));
            String CELL = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("CELL")));

            if ("贷款".equals(contract_attribute)) {
                if (CERT_NO == null){ CERT_NO = ""; }
                if (project == null){ project = ""; }
                if (time == null){ time = ""; }
                if (GMT_CREATED == null){ GMT_CREATED = ""; }
                if (GMT_MODIFIED == null){ GMT_MODIFIED = ""; }
                if (corporate_name == null){ corporate_name = ""; }
                if (contract_attribute == null){ contract_attribute = ""; }
                if (product_name == null){ product_name = ""; }

                String str = "1|" +
                        ID + "|" +
                        CERT_NO + "|" +
                        project + "|" +
                        time + "|" +
                        GMT_CREATED + "|" +
                        GMT_MODIFIED + "|" +
                        corporate_name + "|" +
                        contract_attribute + "|" +
                        product_name;
                context.write(new Text(CERT_NO), new Text(str));
            }

            if (REAL_NAME != null && !"".equals(REAL_NAME)) {
                if (CERT_NO == null){ CERT_NO = ""; }
                if (REAL_NAME == null){ REAL_NAME = ""; }
                if (CELL == null){ CELL = ""; }
                context.write(new Text(CERT_NO), new Text("2|" +
                        REAL_NAME + "|" +
                        CELL));
            }
        }
    }


    public static class MyReducer extends TableReducer<Text, Text, NullWritable> {  //  ImmutableBytesWritable
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            try {
                ArrayList<Map<String, String>> lists1 = new ArrayList<>();
                HashMap<String, String> hashMap2 = new HashMap<>();

                for (Text val : values) {
                    String[] split = val.toString().split("\\|", -1);
                    String flag = split[0]; //  表标记
                    if ("1".equals(flag)) {
                        String ID = split[1]+"_ebaoquan";
                        String CERT_NO = split[2];
                        String project = split[3];
                        String time = split[4];
                        String GMT_CREATED = split[5];
                        String GMT_MODIFIED = split[6];
                        String corporate_name = split[7];
                        String contract_attribute = split[8];
                        String product_name = null;
                        try {
                            product_name = split[9];
                        } catch (Exception e) {
                            e.printStackTrace();
                            Put put = new Put(Bytes.toBytes(ID));
                            put.add("c".getBytes(), "identityNo".getBytes(), Bytes.toBytes(val.toString()));
                            System.out.println("=========================="+val.toString()+"==========================");
                            context.write(NullWritable.get(), put);
                        }
                        HashMap<String, String> hashMap1 = new HashMap<>();
                        hashMap1.put("ID", ID);
                        hashMap1.put("CERT_NO", CERT_NO);
                        hashMap1.put("project", project);
                        hashMap1.put("Etime", time);
                        hashMap1.put("GMT_CREATED", GMT_CREATED);
                        hashMap1.put("GMT_MODIFIED", GMT_MODIFIED);
                        hashMap1.put("corporate_name", corporate_name);
                        hashMap1.put("contract_attribute", contract_attribute);
                        hashMap1.put("product_name", product_name);
                        lists1.add(hashMap1);
                    } else {  //  将表2的数据存入表2集合CERT_NO + "|" +
                        String REAL_NAME = split[1];  //  本人客户号
                        String CELL = split[2];  //  本人名称
                        hashMap2.put("REAL_NAME", REAL_NAME);
                        hashMap2.put("CELL", CELL);
                    }
                }

                for (Map<String, String> map : lists1) {
                    Put put = new Put(Bytes.toBytes(map.get("ID")));
                    map.remove("ID");
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        if ("CERT_NO".equals(entry.getKey())) {
                            put.add("c".getBytes(), "identityNo".getBytes(), Bytes.toBytes(entry.getValue()));
                            continue;
                        }
                        if ("etime".equals(entry.getKey())) {
                            put.add("c".getBytes(), "valueDate".getBytes(), Bytes.toBytes(entry.getValue()));
                            continue;
                        }
                        put.add("c".getBytes(), entry.getKey().getBytes(), Bytes.toBytes(entry.getValue()));
                    }
                    put.add("c".getBytes(), "name".getBytes(), Bytes.toBytes(hashMap2.get("REAL_NAME")));
                    put.add("c".getBytes(), "telephone".getBytes(), Bytes.toBytes(hashMap2.get("CELL")));
                    String age = getCertNoAnalysis(map.get("CERT_NO"));
                    put.add("c".getBytes(), "age".getBytes(), Bytes.toBytes(age));
                    String sex = getSex(map.get("CERT_NO"));
                    put.add("c".getBytes(), "sex".getBytes(), Bytes.toBytes(sex));
                    put.add("c".getBytes(), "source".getBytes(), Bytes.toBytes("易宝全"));
                    put.add("c".getBytes(), "detail_method".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "claimNumber".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "detail_start".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "contractNo".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "detail_source".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "loanAmt".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "maturityDate".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "categoryDisplayName".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "annualRate".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "detail_useing".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "durationDays".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "detail_guarantee".getBytes(), Bytes.toBytes(""));
                    put.add("c".getBytes(), "detail_debtor".getBytes(), Bytes.toBytes("个人"));

                    context.write(NullWritable.get(), put);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getCertNoAnalysis(String CERT_NO) {
        String birthDate = CERT_NO.substring(6, 12);
        String s = null;
        try {
            s = DateUtil.yearsBetween(birthDate, DateUtil.nowString("yyyyMM"), "yyyyMM");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return s;
    }

    public static String getSex(String CERT_NO) {
        String sex = CERT_NO.substring(16, 17);
        if ("1".equals(sex)) {
            return "男";
        } else {
            return "女";
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径
        //创建job
        Job job = new Job(config, "Ebaoquan2HeMr");//job
        job.setJarByClass(Ebaoquan2EmayMr.class);//主类

        List<Scan> scans = new ArrayList<>();
        Scan scan1 = new Scan();
        scan1.setCaching(500);
        scan1.setCacheBlocks(false);
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CERT_NO"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("source"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("time"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("GMT_CREATED"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("GMT_MODIFIED"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("corporate_name"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("contract_attribute"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("product_name"));
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("ebaoquan_24h_formal"));  //  e_qianbao_data_24h_formal e_qianbao_data_1m_test
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setCaching(500);
        scan2.setCacheBlocks(false);
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CERT_NO"));
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("REAL_NAME"));
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CELL"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("sales:sale_user"));
        scans.add(scan2);
        TableMapReduceUtil.initTableMapperJob(scans, Ebaoquan2HeMr.MyMapper.class, Text.class, Text.class, job);

        TableMapReduceUtil.initTableReducerJob("ewe", Ebaoquan2HeMr.MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
