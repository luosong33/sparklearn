package cn.jstorm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.entity.HastenMatching;
import cn.util.ConnectionPool;
import cn.util.CsUtil;
import cn.util.JRedisUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


/**
 * 3、WordCounter同样继承BaseBasicBolt来统计word个数，并将结果写入redis。
 */
public class WordCounter extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    private Logger log = LoggerFactory.getLogger(WordCounter.class);
    Integer id;
    String name;
    Map<String, Integer> counters;

    /**
     * On create  初始化
     */
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
        this.counters = new HashMap<>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
    }

    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the word counters
     * <p>
     * Topology执行完毕的清理工作，比如关闭连接、释放资源等操作都会写在这里
     * 我们用它来打印我们的计数器
     */
    @Override
    public void cleanup() {
        log.info("-- Word Counter [{}-{}] --", name, id);
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            log.info("map[{}:{}]", entry.getKey(), entry.getValue());
            log.info("redis[{}:{}]", entry.getKey(), JRedisUtil.getJRedis().get(entry.getKey()));
            JRedisUtil.getJRedis().del(entry.getKey());
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = input.getString(0);
        log.info(">>>>>>>>>>>>统计：{}<<<<<<<<<<<<<<", str);

//        ReadWriteUtil.write("D:\\tmp\\storm.txt", str, "GBK");
        String clientNo = "";
        String creditNo = "";
        int phoneNum_10 = 0;                       //  3千10天匹配催收库个数
        int callNum_10 = 0;                        //  3千10天匹配催收库次数
        int phoneNum_20 = 0;                       //  3千20天匹配催收库个数
        int callNum_20 = 0;                        //  3千20天匹配催收库次数
        int phoneNum_30 = 0;                       //  3千30天匹配催收库个数
        int callNum_30 = 0;                        //  3千30天匹配催收库次数
        int phoneNum_90 = 0;                       //  3千90天匹配催收库个数
        int callNum_90 = 0;                        //  3千90天匹配催收库次数
        int phoneNum_180 = 0;                      //  3千180天匹配催收库个数
        int callNum_180 = 0;                       //  3千180天匹配催收库次数
        String origDate = "";                      //  原始时间
        String nowDate = "";                       //  入库时间
        Double proportion30 = 0.00;                //  30天风险库通话占总通话人数比
        int top10Num = 0;                          //  运营商通话记录top10撞中催收号码个数
        int phoneNum_90_fortyThousand = 0;         //  4万90天匹配催收库个数
        int phoneNum_180_fortyThousand = 0;        //  4万180天匹配催收库个数

        Set<String> pbset = CsUtil.conn(); //  加载催收库催收机构号码  确认的三千多
        Set<String> pbset_ = CsUtil.conn_(); //  加载催收库催收机构号码  四万

        JSONObject moxieLog = JSONObject.parseObject(str);
        //  解析数据;
        clientNo = moxieLog.getString("clientNo");
        creditNo = moxieLog.getString("creditNo");
        origDate = moxieLog.getString("origDate");
        JSONArray calls = moxieLog.getJSONArray("calls");
        List<String> streamingList = new ArrayList<>();  //  180天借款人通话记录
        Map<String, String> streamingMap = new TreeMap<String, String>(  //  180天借款人通话记录k,v
                new Comparator<String>() {
                    public int compare(String obj1, String obj2) {
                        // 降序排序
                        return obj2.compareTo(obj1);
                    }
                });

        //遍历通话详单jsonArray;
        for (Iterator iterator = calls.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();  //  得到一个通话详单call;
            JSONArray items = job.getJSONArray("items");

            for (Iterator iterator_ = items.iterator(); iterator_.hasNext(); ) {
                JSONObject job_ = (JSONObject) iterator_.next();  //  得到一个通话详单call;
                Set<String> keySet = job_.keySet();
                for (String key : keySet) {
                    if ("peer_number".equals(key)) {
                        String peer_number = String.valueOf(job_.get("peer_number"));
                        String time = String.valueOf(job_.get("time"));
                        streamingList.add(peer_number);
                        streamingMap.put(time, peer_number);
                    }
                }
            }
        }
        String lastString = "";
        Set<String> keySet = streamingMap.keySet();
        Iterator<String> iter = keySet.iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            lastString = streamingMap.get(key);
            break;
        }

        //  运营商top10撞中催收库个数
        HashMap<String, Integer> streamingMap_ = new HashMap<>();
        for (Map.Entry<String, String> entry : streamingMap.entrySet()) {
            if (streamingMap_.containsValue(entry.getValue())) {
                streamingMap_.put(entry.getValue(), streamingMap_.get(entry.getValue()) + 1);
            } else {
                streamingMap_.put(entry.getValue(), 1);
            }
        }
        Map<String, Integer> streamingMap10 = new TreeMap<>(  //  180天借款人通话记录k,v
                new Comparator<String>() {
                    public int compare(String obj1, String obj2) {
                        // 降序排序
                        return obj2.compareTo(obj1);
                    }
                });
        for (String s : pbset_) {
            for (Map.Entry<String, Integer> entry : streamingMap10.entrySet()) {
                if (s.equals(entry.getKey())) {
                    top10Num += 1;
                }
            }
        }

        HastenMatching hastenMatching = new HastenMatching();
        hastenMatching.setClientNo(clientNo);
        hastenMatching.setCreditNo(creditNo);
        hastenMatching.setPhoneNum_10(phoneNum_10 + "");
        hastenMatching.setCallNum_10(callNum_10 + "");
        hastenMatching.setPhoneNum_20(phoneNum_20 + "");
        hastenMatching.setCallNum_20(callNum_20 + "");
        hastenMatching.setPhoneNum_30(phoneNum_30 + "");
        hastenMatching.setCallNum_30(callNum_30 + "");
        hastenMatching.setPhoneNum_90(phoneNum_90 + "");
        hastenMatching.setCallNum_90(callNum_90 + "");
        hastenMatching.setPhoneNum_180(phoneNum_180 + "");
        hastenMatching.setCallNum_180(callNum_180 + "");
        hastenMatching.setOrigDate(origDate);
        hastenMatching.setNowDate(nowDate);
        hastenMatching.setProportion30(proportion30 + "");
        hastenMatching.setTop10Num(top10Num + "");
        hastenMatching.setPhoneNum_180_fortyThousand(phoneNum_180_fortyThousand + "");
        hastenMatching.setPhoneNum_90_fortyThousand(phoneNum_90_fortyThousand + "");
//        CsUtil csUtil = new CsUtil();
        CsUtil.insert(hastenMatching);

        //  不发送到下游插入，直接插入
        /*Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199");
        hconf.set("hbase.zookeeper.property.clientPort", "2181");
        HTable table = null;//  hasten_matching_test
        try {
            table = new HTable(hconf, "hasten_matching_test");
            Put put = new Put(Bytes.toBytes(""));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_10"), Bytes.toBytes(phoneNum_10 + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_10"), Bytes.toBytes(callNum_10 + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_20"), Bytes.toBytes(phoneNum_20 + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_20"), Bytes.toBytes(callNum_20 + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_30"), Bytes.toBytes(phoneNum_30 + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_30"), Bytes.toBytes(callNum_30 + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_90"), Bytes.toBytes(phoneNum_90 + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_90"), Bytes.toBytes(callNum_90 + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_180"), Bytes.toBytes(phoneNum_180 + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_180"), Bytes.toBytes(callNum_180 + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("origDate"), Bytes.toBytes(origDate));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("nowDate"), Bytes.toBytes(nowDate));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("proportion30"), Bytes.toBytes(proportion30));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("top10Num"), Bytes.toBytes(top10Num + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_180_fortyThousand"), Bytes.toBytes(phoneNum_180_fortyThousand + ""));
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_90_fortyThousand"), Bytes.toBytes(phoneNum_90_fortyThousand + ""));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        //        发送到下游hbasebolt
//        collector.emit(new Values(clientNo, creditNo, phoneNum_10, callNum_10, phoneNum_20, callNum_20, phoneNum_30, callNum_30, phoneNum_90, callNum_90,
//                phoneNum_180, callNum_180, origDate, nowDate, proportion30, top10Num, phoneNum_180_fortyThousand, phoneNum_90_fortyThousand));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        发送到下游hbasebolt
//        declarer.declare(new Fields("word"
//                /*"clientNo",
//                "creditNo",
//                "phoneNum_10",
//                "callNum_10",
//                "phoneNum_20",
//                "callNum_20",
//                "phoneNum_30",
//                "callNum_30",
//                "phoneNum_90",
//                "callNum_90",
//                "phoneNum_180",
//                "callNum_180",
//                "origDate",
//                "nowDate",
//                "proportion30",
//                "top10Num",
//                "phoneNum_180_fortyThousand",
//                "phoneNum_90_fortyThousand"*/));
    }

}