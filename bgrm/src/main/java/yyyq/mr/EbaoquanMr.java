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
import yyyq.util.IdWorker;
import yyyq.util.SnowflakeIdGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EbaoquanMr {

    public static class MyMapper extends TableMapper<Text, Text> {
        @Override
        //输入的类型为：key：rowKey； value：一行数据的结果集Result
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            //获取一行数据中的colf：col
            int ID = Bytes.toInt(row.get());

            String CERT_NO = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("CERT_NO")));
            String filename = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("filename")));
            String source = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("source")));
            String recordNumber = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("recordNumber")));
            String time = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("time")));
            String GMT_CREATED = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("GMT_CREATED")));
            String GMT_MODIFIED = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("GMT_MODIFIED")));

            if ("2017".equals(time.substring(0,4))) {
                context.write(new Text(CERT_NO + source), new Text(/*ID + "|" + */CERT_NO + "|" + filename + "|" + source + "|" + recordNumber + "|"
                        + time + "|" + GMT_CREATED + "|" + GMT_MODIFIED));
            }
        }
    }


    public static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Map<String, String>> mapAll = new HashMap<>();
            ArrayList<String> liseDate = new ArrayList<>();
            for (Text val : values) {
                HashMap<String, String> hashMap = new HashMap<>();
                String s = val.toString();
//                ID = s.split("\\|")[0];
                String CERT_NO = s.split("\\|")[0];
                if ("110101196411214519".equals(CERT_NO)) {
                    System.out.println();
                }
                String filename = s.split("\\|")[1];
                String source = s.split("\\|")[2];
                String recordNumber = s.split("\\|")[3];
                String time = s.split("\\|")[4];
                String GMT_CREATED = s.split("\\|")[5];
                String GMT_MODIFIED = s.split("\\|")[6];
//                hashMap.put("ID", ID);
                hashMap.put("CERT_NO", CERT_NO);
                hashMap.put("filename", filename);
                hashMap.put("source", source);
                hashMap.put("recordNumber", recordNumber);
                hashMap.put("time", time);
                hashMap.put("GMT_CREATED", GMT_CREATED);
                hashMap.put("GMT_MODIFIED", GMT_MODIFIED);

                mapAll.put(time, hashMap);
                liseDate.add(time);
            }
            Collections.sort(liseDate);

            ArrayList<String> listDateRes = new ArrayList<>();  //  时间key结果集合
            String timeOne = liseDate.get(0);
            listDateRes.add(timeOne);  //  把第一个加入
            /*int interval = 0;  //  连续的时间开始记录
            for (int i = 0; i < liseDate.size() - 1; i++) {
                String diffs = null;
                try {
                    diffs = DateUtil.getDateDiff(liseDate.get(i), liseDate.get(i + 1), "yyyy-MM-dd HH:mm:ss.sss", "yyyy-MM-dd HH:mm:ss.sss");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String diff = diffs.split("\\|")[2];
                int d = Integer.parseInt(diff);
                if (d <= 23 && interval <= 23) {  //  两两之间小于24，或者连续不到24，都删除
                    listDateRes.remove(liseDate.get(i));
                    interval += d;
                } else {
                    interval = 0;
                }
                listDateRes.add(liseDate.get(i + 1));
            }*/
            for (int i = 0; i < liseDate.size() - 1; i++) {
                String diffs = null;
                try {
//                    diffs = DateUtil.getDateDiff(liseDate.get(i), liseDate.get(i + 1), "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss");
                    diffs = DateUtil.getBetween(liseDate.get(i), liseDate.get(i + 1), "yyyy-MM-dd HH:mm:ss", 0) + "";
                } catch (ParseException e) {
                    e.printStackTrace();
                }
//                String diff = diffs.split("\\|")[2];
                int d = Integer.parseInt(diffs);
                if (d <= 11) {
                    listDateRes.remove(liseDate.get(i));
                }
                listDateRes.add(liseDate.get(i + 1));
            }

            for (String s : listDateRes) {
                Map<String, String> map = mapAll.get(s);
//                IdWorker idWorker = new IdWorker(5);
//                long ID = idWorker.nextId();
                Put put = new Put(Bytes.toBytes(map.get("CERT_NO") + "_" + map.get("time")));
                put.add("c".getBytes(), "CERT_NO".getBytes(), Bytes.toBytes(map.get("CERT_NO")));
                put.add("c".getBytes(), "filename".getBytes(), Bytes.toBytes(map.get("filename")));
                put.add("c".getBytes(), "source".getBytes(), Bytes.toBytes(map.get("source")));
                put.add("c".getBytes(), "recordNumber".getBytes(), Bytes.toBytes(map.get("recordNumber")));
                put.add("c".getBytes(), "time".getBytes(), Bytes.toBytes(map.get("time")));
                put.add("c".getBytes(), "GMT_CREATED".getBytes(), Bytes.toBytes(map.get("GMT_CREATED")));
                put.add("c".getBytes(), "GMT_MODIFIED".getBytes(), Bytes.toBytes(map.get("GMT_MODIFIED")));
                String source_ = getClu(map.get("source"));
                String[] split = source_.split("\\|");
                String s0 = null;
                String s1 = null;
                String s2 = null;
                try {
                    s0 = split[0];
                    s1 = split[1];
                    s2 = split[2];
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("==========" + source_ + "===========");
                }
                if (s0 != null && !"".equals(s0)) {
                    put.add("c".getBytes(), "corporate_name".getBytes(), Bytes.toBytes(s0));
                } else {
                    put.add("c".getBytes(), "corporate_name".getBytes(), Bytes.toBytes(""));
                }
                if (s1 != null && !"".equals(s1)) {
                    put.add("c".getBytes(), "product_name".getBytes(), Bytes.toBytes(s1));
                } else {
                    put.add("c".getBytes(), "product_name".getBytes(), Bytes.toBytes(""));
                }
                if (s2 != null && !"".equals(s2)) {
                    put.add("c".getBytes(), "contract_attribute".getBytes(), Bytes.toBytes(s2));
                } else {
                    put.add("c".getBytes(), "contract_attribute".getBytes(), Bytes.toBytes(""));
                }

                context.write(null, put);
            }
        }
    }

    public static String getClu(String str) {
        if ("米房".equals(str)) {
            return "杭州米房网络技术有限公司|米房借条|贷款";
        }
        if ("长鼎富".equals(str)) {
            return "深圳长鼎富网络金融科技有限公司|魔法现金|贷款";
        }
        if ("亮昕网络".equals(str)) {
            return "上海亮昕网络科技有限公司|魔法现金|贷款";
        }
        if ("俪科网络".equals(str)) {
            return "杭州俪科网络科技有限公司|速钱|贷款";
        }
        if ("银飞利".equals(str)) {
            return "河南银飞利信息技术有限公司|现金磨合/猪手机|贷款";
        }
        if ("微钱".equals(str)) {
            return "浙江微钱信息科技有限公司|微钱贷|贷款";
        }
        if ("聚创商贸".equals(str)) {
            return "温州聚创商贸有限公司|财神钱庄|贷款";
        }
        if ("钱嗖嗖".equals(str)) {
            return "深圳忆往昔金融科技有限公司|钱嗖嗖|贷款";
        }
        if ("我来贷".equals(str)) {
            return "卫盈联信息技术(深圳)有限公司|我来贷|贷款";
        }
        if ("星果".equals(str)) {
            return "星果科技有限公司|九斗鱼|贷款";
        }
        if ("信用钱包".equals(str)) {
            return "北京众信利民信息技术有限公司|信用钱包|贷款";
        }
        if ("现金口贷".equals(str)) {
            return "合肥森巴达信息技术有限公司|现金口贷|贷款";
        }
        if ("榆词网络科技".equals(str)) {
            return "上海榆词网络科技有限公司|借乎|贷款";
        }
        if ("马力钱包".equals(str)) {
            return "廊坊市玖壹网络科技有限公司|马力钱包|贷款";
        }
        if ("壹周金".equals(str)) {
            return "台州壹周金信息咨询有限公司|壹周金|贷款";
        }
        if ("贝多分".equals(str)) {
            return "深圳市贝多分金融科技有限公司|贝多分|贷款";
        }
        if ("中赢金融".equals(str)) {
            return "上海中赢金融信息服务有限公司 |中赢金融/急借通|理财";
        }
        if ("同牛".equals(str)) {
            return "浙江同牛网络科技有限公司|同牛速贷|贷款";
        }
        if ("柏阔".equals(str)) {
            return "杭州柏阔网络科技有限公司|飞鸟贷|贷款";
        }
        if ("速易花".equals(str)) {
            return "西安聚信金融服务有限公司|速易花|贷款";
        }
        if ("好账付".equals(str)) {
            return "恒普商业咨询（深圳）有限公司|好账付|贷款";
        }
        if ("分众小额贷款".equals(str)) {
            return "重庆市分众小额贷款有限公司/上海数禾科技联合运营|还呗|贷款";
        }
        if ("吉财网".equals(str)) {
            return "杭州吉财网络科技有限公司||贷款";
        }
        if ("益芯金融".equals(str)) {
            return "上海益芯金融信息服务有限公司 |益秒到|贷款";
        }
        if ("金融工场".equals(str)) {
            return "北京豆哥投资管理有限公司|金融工场|理财";
        }
        if ("众可贷".equals(str)) {
            return "成都众可电子商务股份有限公司|众可贷|贷款";
        }
        if ("悦才".equals(str)) {
            return "南京悦才信息科技有限公司|悦才掌柜|贷款";
        }
        if ("广州纳明".equals(str)) {
            return "广州市纳明网络科技有限责任公司|拉比鸟钱包|贷款";
        }
        if ("靓号贷".equals(str)) {
            return "温州靓号贷投资有限公司|靓号贷|贷款";
        }
        if ("花点财".equals(str)) {
            return "南昌花点财网络科技有限公司|花点财|贷款";
        }
        if ("桔子分期".equals(str)) {
            return "北京桔子分期电子商务有限公司|桔子分期|贷款";
        }
        if ("重信金融".equals(str)) {
            return "上海重信金融信息服务有限公司|重信金融|贷款";
        }
        if ("盈盈理财".equals(str)) {
            return "杭州龙盈互联网金融信息技术有限公司|盈盈理财|理财";
        }
        if ("炫分期".equals(str)) {
            return "重庆思邦网络股份有限公司|炫分期|贷款";
        }
        if ("北京心向众创".equals(str)) {
            return "北京心向众创科技有限公司|易贷款|贷款";
        }
        if ("优乐花".equals(str)) {
            return "兰州市城关区连汇小额贷款有限责任公司|优乐花|贷款";
        }
        if ("小袋理财".equals(str)) {
            return "上海银来金融信息服务有限公司|小袋理财|理财";
        }
        if ("青年钱包".equals(str)) {
            return "长沙斯谷乐信息技术有限公司|青年钱包|贷款";
        }
        if ("板子科技".equals(str)) {
            return "北京板子科技有限公司|51周转|贷款";
        }
        if ("微钱贷".equals(str)) {
            return "浙江微钱信息科技有限公司|微钱贷|贷款";
        }
        if ("启辰优创".equals(str)) {
            return "黑龙江启辰资产管理有限公司|启辰优创|理财";
        }
        if ("中资联".equals(str)) {
            return "苏州中资联投资管理有限公司|贷我走/贝尔在线|贷款";
        }
        if ("星逸金服".equals(str)) {
            return "||其他";
        }
        if ("和分期".equals(str)) {
            return "安徽和分期网络科技有限公司|和分期|贷款";
        }
        if ("江西普讯".equals(str)) {
            return "江西普讯网络科技有限公司|花米钱包|贷款";
        }
        if ("懒财网".equals(str)) {
            return "北京懒财信息科技有限公司|懒财网|理财";
        }
        if ("汉州资产".equals(str)) {
            return "上海汉州资产管理有限公司|借乎|贷款";
        }
        if ("商银通".equals(str)) {
            return "西安易派软件技术有限公司|商银通|贷款";
        }
        if ("张掖金实惠".equals(str)) {
            return "张掖金实惠网络科技有限公司|实惠袋|贷款";
        }
        if ("分啦科技".equals(str)) {
            return "福州分啦网络科技有限公司||贷款";
        }
        if ("微镑客".equals(str)) {
            return "黑龙江省融之恒投资咨询管理有限公司|微镑客|贷款";
        }
        if ("钱到到".equals(str)) {
            return "北京钱到到金服科技有限公司|钱到到|贷款";
        }
        if ("优时贷".equals(str)) {
            return "|优时贷|贷款";
        }
        if ("互邦金融".equals(str)) {
            return "||其他";
        }
        if ("南京软石".equals(str)) {
            return "南京软石信息科技有限公司|百思贷|理财";
        }
        if ("寻未网络".equals(str)) {
            return "上海寻未网络科技有限公司|小微钱包|贷款";
        }
        if ("百强财富".equals(str)) {
            return "百强贷金融信息服务（北京）有限公司|百强贷|理财";
        }
        if ("任性车贷".equals(str)) {
            return "深圳任尔行科技股份有限公司|任性车贷|贷款";
        }
        if ("分期公司".equals(str)) {
            return "||其他";
        }
        if ("海印金服".equals(str)) {
            return "广东海印集团股份有限公司|海印金服|贷款";
        }
        if ("有融网".equals(str)) {
            return "浙江小融网络科技股份有限公司|有融网|理财";
        }
        if ("天银".equals(str)) {
            return "天银中鑫网络投资发展(深圳)有限公司|亿商宝|其他";
        }
        if ("九仓".equals(str)) {
            return "||其他";
        }
        if ("拿到啦".equals(str)) {
            return "||其他";
        }
        if ("瓜牛".equals(str)) {
            return "兰州市城关区连汇小额贷款有限责任公司/甘肃瓜牛电子商务有限公司|瓜牛钱包|贷款";
        }
        if ("合盈小贷".equals(str)) {
            return "||贷款";
        }
        if ("富龙小贷".equals(str)) {
            return "||贷款";
        }
        if ("尚世同禾".equals(str)) {
            return "北京尚世同禾科技有限公司|尚世同禾|理财";
        }
        if ("乐金所".equals(str)) {
            return "||贷款";
        }
        if ("网利".equals(str)) {
            return "北京网利科技有北京|网利宝|理财";
        }
        if ("信融财富".equals(str)) {
            return "深圳市信融财富投资管理有限公司|信融花花|理财";
        }
        if ("和信贷".equals(str)) {
            return "和信电子商务有限公司|和信贷|理财";
        }
        if ("口袋理财".equals(str)) {
            return "||理财";
        }
        if ("中望金服".equals(str)) {
            return "中望金服信息科技（北京）有限公司|销邦贷|贷款";
        }
        if ("小猪罐子".equals(str)) {
            return "深圳市前海小猪互联网金融服务有限公司|小猪罐子|理财";
        }
        if ("怡人网络".equals(str)) {
            return "||其他";
        }
        if ("汇商所".equals(str)) {
            return "北京汇商金金融服务外包有限公司|汇商所|贷款";
        }
        if ("金融超市".equals(str)) {
            return "||贷款";
        }
        if ("投资达人".equals(str)) {
            return "上海信闳投资管理有限公司|投资达人|其他";
        }
        if ("钱爸爸".equals(str)) {
            return "深圳市钱爸爸电子商务有限公司|钱爸爸|理财";
        }
        if ("金钥匙".equals(str)) {
            return "||其他";
        }
        if ("宜聚网".equals(str)) {
            return "||其他";
        }
        if ("普惠家".equals(str)) {
            return "||其他";
        }
        if ("众隆在线".equals(str)) {
            return "安徽省众隆在线投资管理有限公司|众隆在线|贷款";
        }
        if ("一人一贷".equals(str)) {
            return "||其他";
        }
        if ("富金富".equals(str)) {
            return "||其他";
        }
        if ("个人版".equals(str)) {
            return "||其他";
        }
        if ("淘当铺".equals(str)) {
            return "||其他";
        }
        if ("华銮金融信息".equals(str)) {
            return "||其他";
        }
        if ("新兰德证券投资".equals(str)) {
            return "||其他";
        }
        if ("天财宝".equals(str)) {
            return "||其他";
        }
        if ("1号钱庄".equals(str)) {
            return "深圳前海壹号钱庄金融信息服务有限公司|1号钱庄|理财";
        }
        if ("天玑汇富".equals(str)) {
            return "||其他";
        }
        if ("掌悦理财".equals(str)) {
            return "||其他";
        }
        if ("金财动力".equals(str)) {
            return "||其他";
        }
        if ("海金仓".equals(str)) {
            return "||其他";
        }
        if ("合力贷".equals(str)) {
            return "||其他";
        }
        if ("甘肃悦享".equals(str)) {
            return "||其他";
        }
        if ("菠萝袋".equals(str)) {
            return "||其他";
        }
        if ("普资华企".equals(str)) {
            return "||其他";
        }
        if ("新兰德".equals(str)) {
            return "||其他";
        }
        if ("岁意讯".equals(str)) {
            return "||其他";
        }
        if ("德易融".equals(str)) {
            return "||其他";
        }
        if ("德鸿金融".equals(str)) {
            return "北京德鸿普惠科技有限公司|德鸿金融|理财";
        }
        if ("点贷诚金".equals(str)) {
            return "北京点贷诚金信息技术有限公司|点贷诚金|理财";
        }
        if ("壹宝贷".equals(str)) {
            return "||其他";
        }
        if ("平方贷".equals(str)) {
            return "||其他";
        }
        if ("兴手付".equals(str)) {
            return "||其他";
        }
        if ("易投资".equals(str)) {
            return "||其他";
        }
        if ("寰玥国际".equals(str)) {
            return "||其他";
        }
        if ("融金云".equals(str)) {
            return "||其他";
        }
        if ("乐行理财".equals(str)) {
            return "||理财";
        }
        if ("飞天普惠金融".equals(str)) {
            return "||其他";
        }
        if ("金融在线".equals(str)) {
            return "||其他";
        }
        if ("上海数禾".equals(str)) {
            return "||其他";
        }
        if ("菠萝理财".equals(str)) {
            return "北京朴素磐石投资管理有限公司|菠萝理财|理财";
        }
        if ("8号金融".equals(str)) {
            return "||其他";
        }
        if ("盟利贷".equals(str)) {
            return "||其他";
        }
        if ("上纽信息管理".equals(str)) {
            return "||其他";
        }
        if ("e融所".equals(str)) {
            return "||理财";
        }
        if ("奶瓶儿理财".equals(str)) {
            return "||理财";
        }
        if ("美好分期".equals(str)) {
            return "||贷款";
        }
        if ("花无忧".equals(str)) {
            return "||贷款";
        }
        if ("易七".equals(str)) {
            return "||其他";
        }
        if ("小油菜".equals(str)) {
            return "北京聚融天下信息技术有限公司|小油菜理财|理财";
        }
        if ("聪明投".equals(str)) {
            return "||其他";
        }
        if ("女神钱包".equals(str)) {
            return "||其他";
        }
        if ("小鹭金融".equals(str)) {
            return "上海惊鹭互联网金融信息服务有限公司|小鹭金融|理财";
        }
        if ("钱贷网".equals(str)) {
            return "||其他";
        }
        if ("银胜科技".equals(str)) {
            return "||其他";
        }
        if ("秒贷金融".equals(str)) {
            return "||其他";
        }
        if ("胖胖猪".equals(str)) {
            return "胖胖猪信息咨询服务(北京)有限公司|胖胖猪|理财";
        }
        if ("汇凌金融".equals(str)) {
            return "深圳汇凌金融服务有限公司|汇凌金融|贷款";
        }
        if ("秒贷网".equals(str)) {
            return "||其他";
        }
        if ("微银易贷".equals(str)) {
            return "||其他";
        }
        if ("技术在线".equals(str)) {
            return "||其他";
        }
        if ("未来分期".equals(str)) {
            return "||贷款";
        }
        if ("小宝金融".equals(str)) {
            return "||其他";
        }
        if ("久金所".equals(str)) {
            return "||其他";
        }
        if ("无界财富".equals(str)) {
            return "||其他";
        }
        if ("稳银在线".equals(str)) {
            return "深圳前海久利互联网金融服务有限公司|稳银在线|理财";
        }
        if ("索星金服".equals(str)) {
            return "||其他";
        }
        if ("大麦理财".equals(str)) {
            return "||其他";
        }
        if ("平安永信".equals(str)) {
            return "||其他";
        }
        if ("领投鸟".equals(str)) {
            return "上海吾悠互联网科技服务有限公司|领投鸟|理财";
        }
        if ("天天金融".equals(str)) {
            return "||其他";
        }
        if ("蜗牛分期".equals(str)) {
            return "||贷款";
        }
        if ("贸金所".equals(str)) {
            return "||其他";
        }
        if ("金粮宝".equals(str)) {
            return "||其他";
        }
        if ("锐资贷".equals(str)) {
            return "||其他";
        }
        if ("利基金融".equals(str)) {
            return "||其他";
        }
        if ("原创保".equals(str)) {
            return "||其他";
        }
        if ("天达金融".equals(str)) {
            return "||其他";
        }
        if ("投资无忧".equals(str)) {
            return "||其他";
        }
        if ("九课分期".equals(str)) {
            return "||其他";
        }
        if ("天牛金融".equals(str)) {
            return "||其他";
        }
        if ("浙融资产".equals(str)) {
            return "||其他";
        }
        if ("中鸿".equals(str)) {
            return "||其他";
        }
        if ("利利金服".equals(str)) {
            return "||其他";
        }
        if ("华澳翼时代".equals(str)) {
            return "||其他";
        }
        if ("圈子金服".equals(str)) {
            return "||其他";
        }
        if ("安徽胜辉".equals(str)) {
            return "||其他";
        }
        if ("手机贷".equals(str)) {
            return "||其他";
        }
        if ("易保全".equals(str)) {
            return "||其他";
        }
        if ("温都金服".equals(str)) {
            return "||其他";
        }
        if ("理财去".equals(str)) {
            return "||理财";
        }
        if ("聚信电子".equals(str)) {
            return "||其他";
        }
        if ("钢票网".equals(str)) {
            return "||其他";
        }
        if ("钱程在线".equals(str)) {
            return "||其他";
        }
        if ("一起理财".equals(str)) {
            return "||其他";
        }
        if ("匚匚".equals(str)) {
            return "||其他";
        }
        if ("嗨租".equals(str)) {
            return "||其他";
        }
        if ("天下拉手".equals(str)) {
            return "||其他";
        }
        if ("恩家壹".equals(str)) {
            return "||其他";
        }
        if ("熊猫金服".equals(str)) {
            return "||其他";
        }
        if ("百合贷".equals(str)) {
            return "||其他";
        }
        if ("砚下金融".equals(str)) {
            return "||其他";
        }
        if ("航际数码".equals(str)) {
            return "||其他";
        }
        if ("贝贝投".equals(str)) {
            return "||其他";
        }
        if ("金麦穗互联网金融".equals(str)) {
            return "||其他";
        }
        if ("宝澜君业".equals(str)) {
            return "||其他";
        }
        if ("恒瑞财富网".equals(str)) {
            return "||其他";
        }
        if ("摩码金服".equals(str)) {
            return "||其他";
        }
        if ("早点儿理财".equals(str)) {
            return "||其他";
        }
        if ("上海霈钧信息".equals(str)) {
            return "||其他";
        }
        if ("丰医金融".equals(str)) {
            return "||其他";
        }
        if ("分期吧".equals(str)) {
            return "||其他";
        }
        if ("千米科技".equals(str)) {
            return "||其他";
        }
        if ("半次元".equals(str)) {
            return "||其他";
        }
        if ("学好贷".equals(str)) {
            return "||其他";
        }
        if ("恒信网络科技".equals(str)) {
            return "||其他";
        }
        if ("惠金贷".equals(str)) {
            return "||其他";
        }
        if ("某小贷公司".equals(str)) {
            return "||其他";
        }
        if ("汉州金服".equals(str)) {
            return "||其他";
        }
        if ("福建瑞达星金融".equals(str)) {
            return "||其他";
        }
        if ("衡度".equals(str)) {
            return "||其他";
        }
        if ("赢火虫".equals(str)) {
            return "||其他";
        }
        if ("车宝分".equals(str)) {
            return "||其他";
        }
        if ("金联储".equals(str)) {
            return "||其他";
        }
        if ("e微贷".equals(str)) {
            return "||理财";
        }
        if ("余盆网".equals(str)) {
            return "||其他";
        }
        if ("厚铺街".equals(str)) {
            return "||其他";
        }
        if ("微信公众号".equals(str)) {
            return "||其他";
        }
        if ("渝商宝".equals(str)) {
            return "||其他";
        }
        if ("银信私人财行".equals(str)) {
            return "||其他";
        }
        return "";
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("df.default.name", "hdfs://master:8020/");  //  设置hdfs的默认路径

        Job job = Job.getInstance(conf, "Ebaoquan");
        job.setJarByClass(EbaoquanMr.class);//主类
        //创建scan
        Scan scan = new Scan();
//        scan.setStartRow(Bytes.toBytes(args[0] + "!"));
//        scan.setStopRow(Bytes.toBytes(args[0] + "~"));
        //可以指定查询某一列
//        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("ROW"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CERT_NO"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("filename"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("source"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("recordNumber"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("time"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("GMT_CREATED"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("GMT_MODIFIED"));
        TableMapReduceUtil.initTableMapperJob("ebaoquan", scan, EbaoquanMr.MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("ebaoquan_1y_formal", EbaoquanMr.MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
