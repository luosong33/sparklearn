package cn.jstorm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class MyHBaseBolt extends BaseBasicBolt {
    private Connection connection;
    private Table table;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199");
        hconf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(hconf);  //  示例都是对同一个table进行操作，因此直接将Table对象的创建放在了prepare，在bolt执行过程中可以直接重用。
            table = connection.getTable(TableName.valueOf("hasten_matching_test"));
        } catch (IOException e) {
            e.getStackTrace();
        }
    }
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //从tuple中获取单词
        String clientNo = tuple.getString(0);
        String creditNo = tuple.getString(1);
        String phoneNum_10 = String.valueOf(tuple.getInteger(2));
        String callNum_10 = String.valueOf(tuple.getInteger(3));
        String phoneNum_20 = String.valueOf(tuple.getInteger(4));
        String callNum_20 = String.valueOf(tuple.getInteger(5));
        String phoneNum_30 = String.valueOf(tuple.getInteger(6));
        String callNum_30 = String.valueOf(tuple.getInteger(7));
        String phoneNum_90 = String.valueOf(tuple.getInteger(8));
        String callNum_90 = String.valueOf(tuple.getInteger(9));
        String phoneNum_180 = String.valueOf(tuple.getInteger(10));
        String callNum_180 = String.valueOf(tuple.getInteger(11));
        String origDate = tuple.getString(12);
        String nowDate = tuple.getString(13);
        String proportion30 = tuple.getString(14);
        String top10Num = String.valueOf(tuple.getInteger(15));
        String phoneNum_180_fortyThousand = String.valueOf(tuple.getInteger(16));
        String phoneNum_90_fortyThousand = String.valueOf(tuple.getInteger(17));
        try {
            Put put = new Put(Bytes.toBytes(clientNo + "|" + creditNo));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_10"), Bytes.toBytes(phoneNum_10+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_10"), Bytes.toBytes(callNum_10+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_20"), Bytes.toBytes(phoneNum_20+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_20"), Bytes.toBytes(callNum_20+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_30"), Bytes.toBytes(phoneNum_30+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_30"), Bytes.toBytes(callNum_30+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_90"), Bytes.toBytes(phoneNum_90+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_90"), Bytes.toBytes(callNum_90+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_180"), Bytes.toBytes(phoneNum_180+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_180"), Bytes.toBytes(callNum_180+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("origDate"), Bytes.toBytes(origDate));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("nowDate"), Bytes.toBytes(nowDate));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("proportion30"), Bytes.toBytes(proportion30));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("top10Num"), Bytes.toBytes(top10Num+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_180_fortyThousand"), Bytes.toBytes(phoneNum_180_fortyThousand+""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_90_fortyThousand"), Bytes.toBytes(phoneNum_90_fortyThousand+""));
            table.put(put);
        } catch (IOException e) {
            //do something to handle exception
        }
    }
    @Override
    public void cleanup() {
        //关闭table
        try {
            if(table != null) table.close();
        } catch (Exception e){
            //do something to handle exception
        } finally {
            //在finally中关闭connection
            try {
                connection.close();
            } catch (IOException e) {
                //do something to handle exception
            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //示例中本bolt不向外发射数据，所以没有再做声明
    }
}