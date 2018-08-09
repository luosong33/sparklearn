package yyyq.mr.yzc;

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
import yyyq.mr.yzc.HbaseCellMr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HbaseCellMr_single {

    public static class MyMapper extends TableMapper<Text, Text> {
        @Override
        //输入的类型为：key：rowKey； value：一行数据的结果集Result
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            //获取一行数据中的colf：col
            String linker = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("lINKER")));
            String link_phone = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("LINK_PHONE")));
            String rowkey = Bytes.toString(row.get());
            if (linker != null && !"".equals(linker) && !"null".equals(linker) && link_phone != null && !"".equals(link_phone) && !"null".equals(link_phone)) {
                context.write(new Text(rowkey), new Text(linker + "|" + link_phone));
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
        //  亲属标签
        static String[] relatives = {"爸", "妈", "老婆", "媳妇", "舅", "姨", "姨", "侄", "叔", "伯", "婶", "姑", "哥", "弟", "姐", "妹", "嫂", "儿子", "儿媳", "女儿", "女婿", "外甥", "外孙", "孙女", "孙子", "祖父", "祖母", "外祖父", "外祖母"};
        //  同事标签
        static String[] colleague = {"股长", "线长", "处长", "科长", "厂长", "村长", "乡长", "镇长", "县长", "局长", "部长", "厅长", "主任", "书记", "老板", "领导", "前辈", "师父", "师娘", "师母", "师兄", "师弟", "师姐", "师妹", "师侄", "老师", "同学", "小学", "初中", "高中", "大学"};

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, String> excellentMap = new HashMap<>();  //  优集合
            HashMap<String, String> secondaryMap = new HashMap<>();  //  中集合
            HashMap<String, String> badMap = new HashMap<>();  //  差集合
            String client_no = key.toString().split("_")[1];
            for (Text val : values) {
                String s = val.toString();
                if (s != null && !"".equals(s)) {
                    String linker = s.split("\\|")[0];  //  联系名称
                    String link_phone = val.toString().split("\\|")[1];  //  联系号码
                    for (String rel : relatives) {
                        if (linker.contains(rel)) {
                            excellentMap.put(link_phone, linker);
                        }
                    }
                    for (String rel : colleague) {
                        if (linker.contains(rel)) {
                            badMap.put(link_phone, linker);
                        }
                    }
                }

            }
            for (Map.Entry<String, String> entry : excellentMap.entrySet()) {
                Put put = new Put(Bytes.toBytes(client_no + "_" + entry.getKey()));
                put.add("c".getBytes(), "debtorId".getBytes(), Bytes.toBytes(client_no));
                put.add("c".getBytes(), "name".getBytes(), Bytes.toBytes(entry.getValue()));
                put.add("c".getBytes(), "relation".getBytes(), Bytes.toBytes(entry.getValue()));
                put.add("c".getBytes(), "tag".getBytes(), Bytes.toBytes("优"));
                put.add("c".getBytes(), "order".getBytes(), Bytes.toBytes("1"));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
            }
            for (Map.Entry<String, String> entry : badMap.entrySet()) {
                Put put = new Put(Bytes.toBytes(client_no + "_" + entry.getKey()));
                put.add("c".getBytes(), "debtorId".getBytes(), Bytes.toBytes(client_no));
                put.add("c".getBytes(), "name".getBytes(), Bytes.toBytes(entry.getValue()));
                put.add("c".getBytes(), "relation".getBytes(), Bytes.toBytes(entry.getValue()));
                put.add("c".getBytes(), "tag".getBytes(), Bytes.toBytes("差"));
                put.add("c".getBytes(), "order".getBytes(), Bytes.toBytes("3"));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("df.default.name", "hdfs://master:9000/");//设置hdfs的默认路径
        conf.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径
//        conf.set("hadoop.job.ugi", "hadoop,hadoop");//用户名，组
//        conf.set("mapred.job.tracker", "master:9001");//设置jobtracker在哪

//        Job job = new Job(conf, "HbaseCellMr_single");//job
        Job job = Job.getInstance(conf, "HbaseCellMr_single");
        job.setJarByClass(HbaseCellMr.class);//主类
        //创建scan
        Scan scan = new Scan();
        //可以指定查询某一列
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("LINK_PHONE"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("lINKER"));
        //创建查询hbase的mapper，设置表名、scan、mapper类、mapper的输出key、mapper的输出value    bops_client_cell_linker test3
        TableMapReduceUtil.initTableMapperJob("bops:bops_client_cell_linker", scan, HbaseCellMr.MyMapper.class, Text.class, Text.class, job);
        //创建写入hbase的reducer，指定表名、reducer类、job
        TableMapReduceUtil.initTableReducerJob("contact_person_evaluate_result", HbaseCellMr.MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
