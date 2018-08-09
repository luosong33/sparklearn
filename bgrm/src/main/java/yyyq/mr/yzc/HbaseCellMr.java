package yyyq.mr.yzc;

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
import yyyq.util.DateUtil;
import yyyq.util.StringUtil;

import java.io.IOException;
import java.util.*;

public class HbaseCellMr extends Configured {
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

            String client_no = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("CLIENT_NO")));
            String client_name = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("CLIENT_NAME")));
            String mobile = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("MOBILE")));
            try {
                mobile = new AESDecrypt().getDecrypt(mobile);
            } catch (Exception e) {
                System.out.println("解析错误");
                e.printStackTrace();
            }

            if (clientNo != null && !"".equals(clientNo) && !"null".equals(clientNo)
                    && linker != null && !"".equals(linker) && !"null".equals(linker)
                    && linkPhone != null && !"".equals(linkPhone) && !"null".equals(linkPhone)
                    ) {
                context.write(new Text(clientNo), new Text("1|" + clientNo + "|" + linker + "|" + linkPhone));
            }
            if (!"".equals(client_no) && client_no != null && !"null".equals(client_no)
                    && !"".equals(client_name) && client_name != null && !"null".equals(client_name)
                    && !"".equals(mobile) && mobile != null && !"null".equals(mobile)) {
                context.write(new Text(client_no), new Text("2|" + client_no + "|" + client_name + "|" + mobile));
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
    public static class MyReducer extends TableReducer<Text, Text, NullWritable> {  //  ImmutableBytesWritable
        //  亲属标签
        static String[] relatives = {"爸", "妈", "老婆", "媳妇", "舅", "姨", "姨", "侄", "叔", "伯", "婶", "姑", "哥", "弟", "姐", "妹", "嫂", "儿子", "儿媳", "女儿", "女婿", "外甥", "外孙", "孙女", "孙子", "祖父", "祖母", "外祖父", "外祖母"};
        //  同事标签
        static String[] colleague = {"股长", "线长", "处长", "科长", "厂长", "村长", "乡长", "镇长", "县长", "局长", "部长", "厅长", "主任", "书记", "老板", "领导", "前辈", "师父", "师娘", "师母", "师兄", "师弟", "师姐", "师妹", "师侄", "老师", "同学", "小学", "初中", "高中", "大学"};

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
                        hashMap1.put("phoneNum", phoneNum);
                        lists1.add(hashMap1);
                    } else {  //  将表2的数据存入表2集合
                        String client_no = split[1];  //  本人客户号
                        String client_name = split[2];  //  本人名称
                        String mobile = split[3];  //  本人号码
                        HashMap<String, String> hashMap2 = new HashMap<>();
                        hashMap2.put("client_no", client_no);
                        hashMap2.put("client_name", client_name);
                        hashMap2.put("mobile", mobile);
                        lists2.add(hashMap2);
                    }
                }
            }

            //  对两表进行join
            for (Map<String, String> map2 : lists2) {   //  用户表

                String client_no = map2.get("client_no");  //  用户表客户号
                String client_name = map2.get("client_name");   //  本人姓名
                String mobile = map2.get("mobile"); //  本人手机号
                String oneselfSurname = client_name.substring(0, 1);   //  本人姓
                String oneselfName = client_name.substring(1, client_name.length());   //  本人名
                int oneself = oneselfName.length();   //  本人名数
                for (Map<String, String> map1 : lists1) {   //  通讯录
                    String client_no_link = map1.get("client_no_link");  //  通讯录客户号
                    String linker = map1.get("linker");   //  对方姓名
                    String phoneNum = map1.get("phoneNum"); //  对方手机号
                    String partySurname = linker.substring(0, 1);   //  对方姓
                    String partyName = linker.substring(1, linker.length());   //  对方名
                    int party = partyName.length();   //  对方名数

                    for (String rel : relatives) {  //  优
                        if (linker.contains(rel)) {
                            excellentMap.put(mobile + "_" + phoneNum, client_no + "|" + linker);
                        }
                    }
                    for (String rel : colleague) {  //  差
                        if (linker.contains(rel)) {
                            badMap.put(mobile + "_" + phoneNum, client_no + "|" + linker);
                        }
                    }
                    if (oneselfSurname.equals(partySurname)) {  //  中
                        if (mobile.equals(StringUtil.getNumber(phoneNum)) || client_name.equals(linker) || "自己本人我".contains(linker)) {    //  将本人排除在中中
                            continue;
                        }
                        if (oneself == 1 && party == 1) {   //  AG AQ AE
                            secondaryMap.put(mobile + "_" + phoneNum, client_no + "|" + linker);
                        }
                        if (oneself == 2 && party == 2) {
                            String s1 = oneselfName.substring(0, 1);    //  本人名第一个
                            String s2 = oneselfName.substring(1, 2);    //  本人名第二个
                            String p1 = partyName.substring(0, 1);    //  对方名第一个
                            String p2 = partyName.substring(1, 2);    //  对方名第二个
                            if (s1.equals(p1)) {   //  ABC ABD ABF
                                secondaryMap.put(mobile + "_" + phoneNum, client_no + "|" + linker);
                            }
                            if (s2.equals(p2)) {    //  ADM AHM AOM
                                secondaryMap.put(mobile + "_" + phoneNum, client_no + "|" + linker);
                            }
                            if (s1.equals(s2) && p1.equals(p2)) {    //  ABB AXX ACC
                                secondaryMap.put(mobile + "_" + phoneNum, client_no + "|" + linker);
                            }
                        }
                    }
                }
            }

            int i = 0;
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
                put.add("c".getBytes(), "order".getBytes(), Bytes.toBytes(i + ""));
                put.add("c".getBytes(), "mobile".getBytes(), Bytes.toBytes(mobile));
                put.add("c".getBytes(), "phoneNum".getBytes(), Bytes.toBytes(phoneNum));
                try {
                    context.write(NullWritable.get(), put);  //  null , put
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                i++;
            }
            for (Map.Entry<String, String> entry : secondaryMap.entrySet()) {
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
                put.add("c".getBytes(), "tag".getBytes(), Bytes.toBytes("中"));
                put.add("c".getBytes(), "order".getBytes(), Bytes.toBytes(i + ""));
                put.add("c".getBytes(), "mobile".getBytes(), Bytes.toBytes(mobile));
                put.add("c".getBytes(), "phoneNum".getBytes(), Bytes.toBytes(phoneNum));
//                context.write(new ImmutableBytesWritable(Bytes.toBytes(entry.getKey())), put);
                try {
                    context.write(NullWritable.get(), put);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                i++;
            }
            for (Map.Entry<String, String> entry : badMap.entrySet()) {
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
                put.add("c".getBytes(), "tag".getBytes(), Bytes.toBytes("差"));
                put.add("c".getBytes(), "order".getBytes(), Bytes.toBytes(i + ""));
                put.add("c".getBytes(), "mobile".getBytes(), Bytes.toBytes(mobile));
                put.add("c".getBytes(), "phoneNum".getBytes(), Bytes.toBytes(phoneNum));
                try {
                    context.write(NullWritable.get(), put);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                i++;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径
        //创建job
        Job job = new Job(config, "HbaseCellMr");//job
        job.setJarByClass(HbaseCellMr.class);//主类

        List<Scan> scans = new ArrayList<>();
        Scan scan1 = new Scan();
        scan1.setCaching(500);
        scan1.setCacheBlocks(false);
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("clientNo"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("linkPhone"));
        scan1.addColumn(Bytes.toBytes("c"), Bytes.toBytes("linker"));
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("ods_cell_linker"));  //  通讯录
        String yyyyMMdd = DateUtil.getFrontAfterDate(DateUtil.nowString("yyyyMMdd"), -1, "yyyyMMdd");
        String startRow = yyyyMMdd;
        String endRow = yyyyMMdd;
        if (args.length > 0) {
            startRow = DateUtil.getDateString(args[0], "yyyy-MM-dd", "yyyyMMdd");
            endRow = DateUtil.getDateString(args[1], "yyyy-MM-dd", "yyyyMMdd");
        }
        scan1.setStartRow(Bytes.toBytes("1001" + startRow + "!"));  //  100120141114000005_(408) 555-5270
        scan1.setStopRow(Bytes.toBytes("1001" + endRow + "~"));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setCaching(500);
        scan2.setCacheBlocks(false);
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CLIENT_NO"));
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("MOBILE"));
        scan2.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CLIENT_NAME"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("bops:bops_client"));  //  用户表
        String yyyyMMdd2 = DateUtil.getFrontAfterDate(DateUtil.nowString("yyyyMMdd"), -1, "yyyyMMdd");
        String startRow2 = yyyyMMdd2;
        String endRow2 = yyyyMMdd2;
        if (args.length > 0) {
            startRow2 = DateUtil.getDateString(args[0], "yyyy-MM-dd", "yyyyMMdd");
            endRow2 = DateUtil.getDateString(args[1], "yyyy-MM-dd", "yyyyMMdd");
        }
        scan2.setStartRow(Bytes.toBytes("!_1001" + startRow2 + "!"));  //  2014-11-14 00:45:05.0_100120141114000005  "!_1001" +
        scan2.setStopRow(Bytes.toBytes("~_1001" + endRow2 + "~"));  //  支持模糊匹配  不用  "~_1001" +  依然可以
        scans.add(scan2);
        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);

        TableMapReduceUtil.initTableReducerJob("contact_person", MyReducer.class, job);   // contact_person_evaluate_result   test4
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}