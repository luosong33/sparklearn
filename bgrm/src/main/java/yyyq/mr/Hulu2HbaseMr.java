package yyyq.mr;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import yyyq.util.StringUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Hulu2HbaseMr extends Configured {
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
//            String rowkey = Bytes.toString(row.get());
//            String client_no_link = rowkey.split("_")[1];
            String client_no_link = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("CLIENT_NO")));
            String linker = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("lINKER")));
            String link_phone = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("LINK_PHONE")));

            String client_no = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("CLIENT_NO")));
            String mobile = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("MOBILE")));
            try {
                mobile = new AESDecrypt().getDecrypt(mobile);
            } catch (Exception e) {
                System.out.println("解析错误");
                e.printStackTrace();
            }
            String client_name = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("CLIENT_NAME")));
            /*if (StringUtils.isNotBlank(mobile)){
                System.out.println("========mobile========" + mobile);
            }*/

            if (client_no_link != null && !"".equals(client_no_link) && !"null".equals(client_no_link)
                    && linker != null && !"".equals(linker) && !"null".equals(linker)
                    && link_phone != null && !"".equals(link_phone) && !"null".equals(link_phone)
                    ) {
                context.write(new Text(client_no_link), new Text("1|" + client_no_link + "|" + linker + "|" + link_phone));
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
            HashMap<String, String> excellentMap = new HashMap<>();  //  优集合
            HashMap<String, String> secondaryMap = new HashMap<>();  //  中集合
            HashMap<String, String> badMap = new HashMap<>();  //  差集合
            ArrayList<Map<String, String>> lists1 = new ArrayList<>();   //  通讯录
            ArrayList<Map<String, String>> lists2 = new ArrayList<>();   //  用户表
            for (Text val : values) {
                String[] split = val.toString().split("\\|");
                String flag = split[0]; //  表标记
                if (split.length == 4) {
                    if ("1".equals(flag)) {     //  将表1的数据存入表1集合
                        String client_no_link = split[1];  //  联系人对应客户号
                        String linker = split[2];  //  联系人名称
                        String phoneNum = split[3];  //  联系人号码
                        HashMap<String, String> hashMap1 = new HashMap<>();
                        hashMap1.put("client_no_link", client_no_link);
                        hashMap1.put("linker", linker);
                        lists1.add(hashMap1);
                    }
                }
            }

            for (Map.Entry<String, String> entry : excellentMap.entrySet()) {
                String[] ks = entry.getKey().split("_");
                String[] vs = entry.getValue().split("\\|");
                String mobile = ks[0];
                String phoneNum = ks[1];
                String client_no = vs[0];
                String linker = vs[1];
                Put put = new Put(Bytes.toBytes(entry.getKey()));
                put.add("c".getBytes(), "debtorId".getBytes(), Bytes.toBytes(client_no));
                put.add("c".getBytes(), "name".getBytes(), Bytes.toBytes(linker));
                put.add("c".getBytes(), "relation".getBytes(), Bytes.toBytes(linker));
                put.add("c".getBytes(), "tag".getBytes(), Bytes.toBytes("优"));
                put.add("c".getBytes(), "order".getBytes(), Bytes.toBytes("1"));
                put.add("c".getBytes(), "phoneNum".getBytes(), Bytes.toBytes(phoneNum));
                context.write(null, put);
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        config.set("hbase.zookeeper.property.clientPort", "2181");
//        config.set("df.default.name", "hdfs://master:9000/");//设置hdfs的默认路径
        config.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径
//        config.set("hadoop.job.ugi", "hadoop,hadoop");//用户名，组
//        config.set("mapred.job.tracker", "master:9001");//设置jobtracker在哪
        //创建job
        Job job = new Job(config, "HbaseCellMr");//job
        job.setJarByClass(Hulu2HbaseMr.class);//主类
        /*Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("LINK_PHONE"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("lINKER"));*/

        List<Scan> scans = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        String yesterday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.s").format(cal.getTime());

        Scan scan1 = new Scan();
        scan1.setCaching(500);
        scan1.setCacheBlocks(false);
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CLIENT_NO"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("LINK_PHONE"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("lINKER"));
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("bops:test"));
        scans.add(scan1);

        Scan scan2 = new Scan();
        /*scan2.setStartRow(Bytes.toBytes(yesterday+"_#"));
        scan2.setStopRow(Bytes.toBytes(yesterday+"_:"));
        if(args.length==1){
            scan2.setStartRow(Bytes.toBytes(args[0]+"_#"));
            scan2.setStopRow(Bytes.toBytes(args[0]+"_:"));
        }*/
        scan2.setCaching(500);
        scan2.setCacheBlocks(false);
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CLIENT_NO"));
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("MOBILE"));
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CLIENT_NAME"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("bops:bops_client"));
        scans.add(scan2);
        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);

        //创建查询hbase的mapper，设置表名、scan、mapper类、mapper的输出key、mapper的输出value    bops_client_cell_linker test
//        TableMapReduceUtil.initTableMapperJob("bops:bops_client_cell_linker", scan, MyMapper.class, Text.class, Text.class, job);
        //创建写入hbase的reducer，指定表名、reducer类、job
        TableMapReduceUtil.initTableReducerJob("test4", MyReducer.class, job);   // contact_person_evaluate_result   test4
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}