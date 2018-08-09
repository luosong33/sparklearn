package cn.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


/**
 * 准备工作都做好了，只差Topology来运行了。
 */
public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {

       //Topology definition  定义一个Topology  
        TopologyBuilder builder = new TopologyBuilder();
        //一个spout读取文本
        builder.setSpout("word-reader",new WordReader());
        //一个bolt用来标准化单词
//        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        //一个bolt为单词计数
//        builder.setBolt("word-counter", new WordCounter(),1).fieldsGrouping("word-normalizer", new Fields("word"));
        builder.setBolt("word-counter", new WordCounter(),1).fieldsGrouping("word-reader", new Fields("word"));

//        builder.setBolt("word-hbase", new MyHBaseBolt(),1).fieldsGrouping("hbase-reader", new Fields("word"));

        //Configuration 配置
        Config conf = new Config();

        //kafka消费者配置
        conf.put("bootstrapServers", "192.168.15.195:9092,192.168.15.196:9092,192.168.15.197:9092,192.168.15.198:9092,192.168.15.199:9092");
        conf.put("topic", "MOXIE_CARRIER_ACCESS_SUMMIT_REPEAT");
        conf.put("groupId", "TopologyMain");

        conf.setDebug(false);
        //建议加上这行，使得每个bolt/spout的并发度都为1
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        //创建一个本地模式cluster
        LocalCluster cluster = new LocalCluster();

        //提交拓扑
        cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());

        //等待1分钟， 1分钟后会停止拓扑和集群， 视调试情况可增大该数值
        Thread.sleep(60*1000*5);        

        System.out.println(">>>>>>>>>>>>>>>>>>>结束拓扑<<<<<<<<<<<<<<<<<");
        //结束拓扑
        cluster.killTopology("Getting-Started-Toplogie");
        System.out.println(">>>>>>>>>>>>>>>>>>>关闭连接<<<<<<<<<<<<<<<<<");
        cluster.shutdown();
    }
}