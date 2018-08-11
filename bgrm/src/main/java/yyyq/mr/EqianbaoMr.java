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

public class EqianbaoMr {

    public static class MyMapper extends TableMapper<Text, Text> {
        @Override
        //输入的类型为：key：rowKey； value：一行数据的结果集Result
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            //获取一行数据中的colf：col
            String ID = Bytes.toString(row.get());
            String CERT_NO = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("CERT_NO")));
            String type = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("type")));
            String project = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("project")));
            String Etime = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("Etime")));
            String evidenceId = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("evidenceId")));
            String GMT_CREATED = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("GMT_CREATED")));
            String GMT_MODIFIED = Bytes.toString(value.getValue(Bytes.toBytes("c"), Bytes.toBytes("GMT_MODIFIED")));

            context.write(new Text(CERT_NO + project), new Text(ID + "|" + CERT_NO + "|" + type + "|" + project + "|" + Etime + "|"
                    + evidenceId + "|" + GMT_CREATED + "|" + GMT_MODIFIED));
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
            Map<String, Map<String, String>> mapAll = new HashMap<>();
            ArrayList<String> liseDate = new ArrayList<>();
            for (Text val : values) {
                HashMap<String, String> hashMap = new HashMap<>();
                String s = val.toString();
                String ID = s.split("\\|")[0];
                String CERT_NO = s.split("\\|")[1];
                String type = s.split("\\|")[2];
                String project = s.split("\\|")[3];
                String Etime = s.split("\\|")[4];
                String evidenceId = s.split("\\|")[5];
                String GMT_CREATED = s.split("\\|")[6];
                String GMT_MODIFIED = s.split("\\|")[7];
                hashMap.put("ID", ID);
                hashMap.put("CERT_NO", CERT_NO);
                hashMap.put("type", type);
                hashMap.put("project", project);
                hashMap.put("Etime", Etime);
                hashMap.put("evidenceId", evidenceId);
                hashMap.put("GMT_CREATED", GMT_CREATED);
                hashMap.put("GMT_MODIFIED", GMT_MODIFIED);

                mapAll.put(Etime, hashMap);
                liseDate.add(Etime);
            }

            Collections.sort(liseDate);
            ArrayList<String> listDateRes = new ArrayList<>();

            listDateRes.add(liseDate.get(0));  //  把第一个加入
            for (int i = 0; i < liseDate.size() - 1; i++) {
                String diffs = null;
                try {
//                    diffs = DateUtil.getDateDiff(liseDate.get(i), liseDate.get(i + 1), "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss");
                    diffs = DateUtil.getBetween(liseDate.get(i), liseDate.get(i + 1), "yyyy-MM-dd HH:mm:ss", 0)+"";
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
                Put put = new Put(Bytes.toBytes(map.get("ID")));
                put.add("c".getBytes(), "CERT_NO".getBytes(), Bytes.toBytes(map.get("CERT_NO")));
                put.add("c".getBytes(), "type".getBytes(), Bytes.toBytes(map.get("type")));
                put.add("c".getBytes(), "project".getBytes(), Bytes.toBytes(map.get("project")));
                put.add("c".getBytes(), "Etime".getBytes(), Bytes.toBytes(map.get("Etime")));
                put.add("c".getBytes(), "evidenceId".getBytes(), Bytes.toBytes(map.get("evidenceId")));
                put.add("c".getBytes(), "GMT_CREATED".getBytes(), Bytes.toBytes(map.get("GMT_CREATED")));
                put.add("c".getBytes(), "GMT_MODIFIED".getBytes(), Bytes.toBytes(map.get("GMT_MODIFIED")));
                String project = getClu(map.get("project"));
                String[] split = project.split("\\|");
                String s0 = null;
                String s1 = null;
                String s2 = null;
                try {
                    s0 = split[0];
                    s1 = split[1];
                    s2 = split[2];
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("=========="+project+"===========");
                }
                if (s0 != null && !"".equals(s0)) {
                    put.add("c".getBytes(), "corporate_name".getBytes(), Bytes.toBytes(s0));
                }else {
                    put.add("c".getBytes(), "corporate_name".getBytes(), Bytes.toBytes(""));
                }
                if (s1 != null && !"".equals(s1)) {
                    put.add("c".getBytes(), "product_name".getBytes(), Bytes.toBytes(s1));
                }else {
                    put.add("c".getBytes(), "product_name".getBytes(), Bytes.toBytes(""));
                }
                if (s2 != null && !"".equals(s2)) {
                    put.add("c".getBytes(), "contract_attribute".getBytes(), Bytes.toBytes(s2));
                }else {
                    put.add("c".getBytes(), "contract_attribute".getBytes(), Bytes.toBytes(""));
                }

                context.write(null, put);
            }
        }
    }

    public static String getClu(String str) {
        if ("北京掌众金融信息服务有限公司".equals(str)){ return "北京掌众金融信息服务有限公司|闪电借款|贷款"; }
        if ("有贝电子合约".equals(str)){ return "有贝网络科技(杭州)有限公司|科技服务|贷款"; }
        if ("天机b端".equals(str)){ return "北京融世纪信息技术有限公司|融360|贷款"; }
        if ("现金文档保全业务".equals(str)){ return "微额速达（上海）金融信息服务有限公司|现金巴士|贷款"; }
        if ("通用合同".equals(str)){ return "杭州铜米互联网金融服务有限公司|铜掌柜|贷款"; }
        if ("简融".equals(str)){ return "杭州蜂融网络科技有限公司|简融现金贷款王|贷款"; }
        if ("布丁小贷".equals(str)){ return "杭州行健金融服务外包有限公司|布丁小贷|贷款"; }
        if ("明特金融".equals(str)){ return "明特商业保理有限公司|向钱贷|贷款"; }
        if ("华融消费金融".equals(str)){ return "华融消费金融公司|华融贷|贷款"; }
        if ("人众任务中心".equals(str)){ return "浙江人众金融服务股份有限公司|人众金服|理财"; }
        if ("钱盆网".equals(str)){ return "广西钱盆科技股份有限公司|钱盆网|贷款"; }
        if ("盈盈易贷".equals(str)){ return "杭州盈火网络科技有限公司|盈盈易贷|贷款"; }
        if ("万达渤海项目".equals(str)){ return "上海万达小额贷款有限公司|万达贷|贷款"; }
        if ("佐力E签宝签约系统".equals(str)){ return "佐力科创小额贷款股份有限公司|佐力小贷|贷款"; }
        if ("数禾科技".equals(str)){ return "上海数禾信息科技有限公司|还呗|贷款"; }
        if ("开心钱包".equals(str)){ return "上海灿福信息科技发展有限公司|开心钱包|贷款"; }
        if ("贷小强".equals(str)){ return "长沙鸿泽信息科技有限公司|贷小强|贷款"; }
        if ("多多付钱包".equals(str)){ return "珠海市天信恒创科技有限公司|多多付钱包|贷款"; }
        if ("有用分期".equals(str)){ return "深圳天道计然金融服务有限公司|有用分期|贷款"; }
        if ("真金服".equals(str)){ return "北京惠人惠民科技技术有限公司|真金服|理财"; }
        if ("宋江贷".equals(str)){ return "杭州琅格金融信息服务有限公司|宋江贷|贷款"; }
        if ("人人花".equals(str)){ return "深圳人人花网络科技有限公司|人人花|贷款"; }
        if ("还借钱".equals(str)){ return "北京嗨亚财富信息技术有限公司|还借钱|贷款"; }
        if ("信用钱包".equals(str)){ return "北京众信利民信息技术有限公司|信用钱包|贷款"; }
        if ("纵横易贷".equals(str)){ return "浙江纵横新创投资集团有限公司|纵横易贷|贷款"; }
        if ("爱学贷正式环境".equals(str)){ return "杭州爱财网络科技有限公司|爱又米|贷款"; }
        if ("信融财富".equals(str)){ return "深圳市信融财富投资管理有限公司|信融花花|贷款"; }
        if ("爱贷借款app".equals(str)){ return "浙江爱贷金融服务外包股份有限公司|爱贷借款|贷款"; }
        if ("信息服务合同".equals(str)){ return "东莞市银众实业投资有限公司|快来贷|贷款"; }
        if ("E代付".equals(str)){ return "深圳超富资产管理有限公司|E代付|贷款"; }
        if ("纷信信用".equals(str)){ return "杭州霖梓网络科技有限公司|纷信信用|贷款"; }
        if ("惠今业务平台".equals(str)){ return "深圳市惠今惠信金融有限公司|惠今惠众|贷款"; }
        if ("51反呗".equals(str)){ return "浙江阿拉丁电子商务股份有限公司|51反呗|贷款"; }
        if ("草根投资".equals(str)){ return "浙江草根网络科技有限公司|草根投资|理财"; }
        if ("海投汇".equals(str)){ return "北京信宏时代文化传播有限责任公司|海投汇|理财"; }
        if ("任我花".equals(str)){ return "上海任我花网络科技有限公司|任我花|贷款"; }
        if ("电子合同签署".equals(str)){ return "北京融智||其他"; }
        if ("通天贷".equals(str)){ return "成武县夜舞网络科技有限公司/南通浪花电子商务有限公司/长沙鸿泽|通天贷|贷款"; }
        if ("速领薪".equals(str)){ return "深圳市鼎鑫源互联网金融服务有限公司|速领薪|贷款"; }
        if ("钱到".equals(str)){ return "杭州钱趣金融信息服务有限公司|钱到|贷款"; }
        if ("小树时代e签宝（借款&理财）".equals(str)){ return "深圳前海小树时代互联网金融服务有限公司|小树时代|贷款"; }
        if ("放心花".equals(str)){ return "深圳市众利财富管理有限公司|放心花|贷款"; }
        if ("7贷".equals(str)){ return "深圳市前海龙汇通互联网金融服务有限公司|7贷金融|贷款"; }
        if ("诺秒贷".equals(str)){ return "泰来融富小额贷款有限公司|诺秒贷|贷款"; }
        if ("信闪贷".equals(str)){ return "深圳信用宝金融服务有限公司|信闪贷（信用宝）|贷款"; }
        if ("诺远科技".equals(str)){ return "诺远科技发展有限公司|小诺理财|理财"; }
        if ("学票".equals(str)){ return "深圳威马逊信息技术有限公司|学票|贷款"; }
        if ("快乐达".equals(str)){ return "快睿登信息科技（上海）有限公司|快乐达|贷款"; }
        if ("马贷来了".equals(str)){ return "杭州投融谱华互联网金融服务有限公司|马贷来了|贷款"; }
        if ("猎豹极速贷".equals(str)){ return "北京猎豹网络科技有限公司|猎豹极速贷|贷款"; }
        if ("元宝e家".equals(str)){ return "元宝亿家互联网信息服务（北京）有限公司|元宝e家|贷款"; }
        if ("花易借".equals(str)){ return "花易借经济信息咨询有限公司|花易借|贷款"; }
        if ("百事通".equals(str)){ return "||其他"; }
        if ("阿里金服".equals(str)){ return "前海阿里金融服务(深圳)有限公司|易分期、大圣分期|贷款"; }
        if ("盒子小贷".equals(str)){ return "||贷款"; }
        if ("小葱钱包".equals(str)){ return "上海蜗居互联网金融信息服务有限公司|小葱钱包|贷款"; }
        if ("瓜子售车合同".equals(str)){ return "金瓜子科技发展（北京）有限公司|瓜子二手车|其他"; }
        if ("金汇E签宝".equals(str)){ return "深圳金汇财富金融服务有限公司|金汇金融|理财"; }
        if ("联银财富".equals(str)){ return "北京联银信息技术有限公司|联银财富|贷款"; }
        if ("泰隆理财".equals(str)){ return "浙江诚隆金融信息服务有限公司|泰隆理财|理财"; }
        if ("开始钱包".equals(str)){ return "即可开始互联网信息服务（北京）有限公司|开始钱包|贷款"; }
        if ("爱本地".equals(str)){ return "深圳首金誉互联网金融服务有限公司|爱本地|理财"; }
        if ("广东华兴银行".equals(str)){ return "广东华兴银行股份有限公司|广东华兴银行|贷款"; }
        if ("汇投网银行存管".equals(str)){ return "汇投(北京)金融信息服务有限公司|汇投网|理财"; }
        if ("风控管理审核系统".equals(str)){ return "||其他"; }
        if ("信美分期".equals(str)){ return "深圳前海信美分期科技有限公司|信美分期|贷款"; }
        if ("理想宝".equals(str)){ return "深圳市前海理想金融控股有限公司|理想宝|理财"; }
        if ("懒投资".equals(str)){ return "北京大家玩科技有限公司 |懒投资|理财"; }
        if ("深圳前海华人互联网金融服务集团有限公司".equals(str)){ return "深圳前海华人互联网金融服务集团有限公司|华人金融|理财"; }
        if ("资金宝".equals(str)){ return "深圳市前海理想金融控股有限公司|资金宝|贷款"; }
        if ("房速贷".equals(str)){ return "上海以房互联网科技有限公司|房速贷|贷款"; }
        if ("新升贷".equals(str)){ return "浙江万维金融信息服务有限公司|新升贷|理财"; }
        if ("Ucar".equals(str)){ return "深圳优卡南方能源有限公司|优卡白条|贷款"; }
        if ("出粮".equals(str)){ return "前海远方|出粮|贷款"; }
        if ("首E家".equals(str)){ return "北京首创金融资产交易信息服务股份有限公司|首E家|理财"; }
        if ("借吧".equals(str)){ return "爱筹科技有限公司|借吧|贷款"; }
        if ("汇诚金服".equals(str)){ return "浙江恩诚网络科技有限公司|汇诚金服|理财"; }
        if ("洋葱借条".equals(str)){ return "杭州贝途科技有限公司|洋葱借条|贷款"; }
        if ("投融家".equals(str)){ return "杭州投融谱华互联网金融服务有限公司|投融家|理财"; }
        if ("人人爱家合同签名".equals(str)){ return "||理财"; }
        if ("极速6秒".equals(str)){ return "||贷款"; }
        if ("kuaiqian".equals(str)){ return "||贷款"; }
        if ("爱投金融".equals(str)){ return "||贷款"; }
        if ("唯品金融".equals(str)){ return "||贷款"; }
        if ("壹贰钱包".equals(str)){ return "||贷款"; }
        if ("微额盒子".equals(str)){ return "||贷款"; }
        if ("钱来网".equals(str)){ return "||贷款"; }
        if ("微米签章".equals(str)){ return "||其他"; }
        if ("信易闪借".equals(str)){ return "||贷款"; }
        if ("大丰收金融".equals(str)){ return "||理财"; }
        if ("消费金融事业部".equals(str)){ return "||贷款"; }
        if ("吉屋小贷H5".equals(str)){ return "||贷款"; }
        if ("网投网p2p".equals(str)){ return "||理财"; }
        if ("1111563886".equals(str)){ return "||其他"; }
        if ("友金所".equals(str)){ return "||理财"; }
        if ("社交赢行".equals(str)){ return "||理财"; }
        if ("大搜车".equals(str)){ return "||其他"; }
        if ("佳缘金融".equals(str)){ return "||贷款"; }
        if ("团贷网".equals(str)){ return "||理财"; }
        if ("平安租赁极租客".equals(str)){ return "||贷款"; }
        if ("浙江佰财金融信息服务有限公司".equals(str)){ return "||理财"; }
        if ("巴巴汇".equals(str)){ return "||贷款"; }
        if ("顺网邦全".equals(str)){ return "||贷款"; }
        if ("爱上租电子合同".equals(str)){ return "||其他"; }
        if ("双瑞惠花".equals(str)){ return "||贷款"; }
        if ("润龙惠花".equals(str)){ return "||贷款"; }
        if ("百信金服".equals(str)){ return "||贷款"; }
        if ("联投金融".equals(str)){ return "||贷款"; }
        if ("砖头网".equals(str)){ return "||贷款"; }
        if ("美的金融中心".equals(str)){ return "||贷款"; }
        if ("信联金融平台".equals(str)){ return "||理财"; }
        if ("石时代".equals(str)){ return "||理财"; }
        if ("银盛金服".equals(str)){ return "||贷款"; }
        if ("深圳市前海小猪互联网金融服务有限公司".equals(str)){ return "||理财"; }
        if ("活励贷".equals(str)){ return "||贷款"; }
        if ("红璞青年公寓".equals(str)){ return "||其他"; }
        if ("正勤金融".equals(str)){ return "||贷款"; }
        if ("杭州联合银行".equals(str)){ return "||贷款"; }
        if ("尚朋高科".equals(str)){ return "||其他"; }
        if ("兴业消金空手到".equals(str)){ return "||贷款"; }
        if ("佳缘一对一".equals(str)){ return "||其他"; }
        if ("德鸿金融".equals(str)){ return "||理财"; }
        if ("e签宝ios版本正式环境".equals(str)){ return "||其他"; }
        if ("酷盈网".equals(str)){ return "||贷款"; }
        if ("极小贷".equals(str)){ return "||贷款"; }
        if ("ealicai".equals(str)){ return "||其他"; }
        if ("周转在线".equals(str)){ return "||其他"; }
        if ("信息咨询服务合同".equals(str)){ return "||其他"; }
        if ("yoho8_1".equals(str)){ return "||其他"; }
        if ("引行金融信贷管理系统".equals(str)){ return "||贷款"; }
        if ("八戒小贷公章".equals(str)){ return "||贷款"; }
        if ("农发贷".equals(str)){ return "||贷款"; }
        if ("优贷代理".equals(str)){ return "||贷款"; }
        if ("固金所".equals(str)){ return "||贷款"; }
        if ("初代".equals(str)){ return "||其他"; }
        if ("YY学车".equals(str)){ return "||贷款"; }
        if ("线上签章".equals(str)){ return "||其他"; }
        if ("易港金融".equals(str)){ return "||贷款"; }
        if ("租了么".equals(str)){ return "||贷款"; }
        if ("电梯云管家".equals(str)){ return "||贷款"; }
        if ("云贷365征信授权签名".equals(str)){ return "||贷款"; }
        if ("铂恒金服".equals(str)){ return "||贷款"; }
        if ("商户贷".equals(str)){ return "||其他"; }
        if ("电子签章系统".equals(str)){ return "||其他"; }
        if ("她金控".equals(str)){ return "||贷款"; }
        if ("博能快捷签".equals(str)){ return "||贷款"; }
        if ("欢乐合家理财".equals(str)){ return "||贷款"; }
        if ("E签宝WEB项目-V2.21".equals(str)){ return "||其他"; }
        if ("维权骑士".equals(str)){ return "||贷款"; }
        if ("金融博士".equals(str)){ return "||贷款"; }
        if ("米金社".equals(str)){ return "||贷款"; }
        if ("E签宝安卓客户端正式版(1.0.30 )".equals(str)){ return "||其他"; }
        if ("北京天使汇".equals(str)){ return "||贷款"; }
        if ("工商注册项目".equals(str)){ return "||其他"; }
        if ("弹个车".equals(str)){ return "||贷款"; }
        if ("安快金融".equals(str)){ return "||贷款"; }
        if ("易代付".equals(str)){ return "||贷款"; }
        if ("龙贷理财".equals(str)){ return "||贷款"; }
        if ("民投金服".equals(str)){ return "||贷款"; }
        if ("开始众筹签名".equals(str)){ return "||贷款"; }
        if ("金融大数据引擎".equals(str)){ return "||贷款"; }
        if ("众安科技".equals(str)){ return "||贷款"; }
        if ("共享贷".equals(str)){ return "||贷款"; }
        if ("云莱坞".equals(str)){ return "||贷款"; }
        if ("现金骑士".equals(str)){ return "||贷款"; }
        if ("DMCC签章".equals(str)){ return "||其他"; }
        if ("e签宝-云投汇".equals(str)){ return "||贷款"; }
        if ("麦家_云平台".equals(str)){ return "||贷款"; }
        if ("重庆中旅安信".equals(str)){ return "||贷款"; }
        if ("知商金融".equals(str)){ return "||贷款"; }
        if ("恒富在线合同签署".equals(str)){ return "||贷款"; }
        if ("成都鸿学金信商务咨询有限公司".equals(str)){ return "||贷款"; }
        if ("e合同H5".equals(str)){ return "||其他"; }
        if ("即刻贷".equals(str)){ return "||贷款"; }
        if ("百金普惠".equals(str)){ return "||贷款"; }
        if ("元泰金服".equals(str)){ return "||贷款"; }
        if ("金蜂财富".equals(str)){ return "||贷款"; }
        if ("共信赢金服".equals(str)){ return "||贷款"; }
        if ("聚宝珠".equals(str)){ return "||贷款"; }
        if ("天使汇".equals(str)){ return "||贷款"; }
        if ("好买电子签名平台".equals(str)){ return "||贷款"; }
        if ("友金普惠".equals(str)){ return "||贷款"; }
        if ("多融财富".equals(str)){ return "||贷款"; }
        if ("秒针理财".equals(str)){ return "||贷款"; }
        if ("南京银洲金服".equals(str)){ return "||贷款"; }
        if ("糯米袋".equals(str)){ return "||贷款"; }
        if ("易借条".equals(str)){ return "||贷款"; }
        if ("引行金融".equals(str)){ return "||贷款"; }
        if ("马甲袋".equals(str)){ return "||贷款"; }
        if ("昆山农商行".equals(str)){ return "||贷款"; }
        if ("微贷工厂".equals(str)){ return "||贷款"; }
        if ("中邦合同签署".equals(str)){ return "||贷款"; }
        if ("恒享分期".equals(str)){ return "||贷款"; }
        if ("V血拼".equals(str)){ return "||贷款"; }
        if ("租房平台".equals(str)){ return "||贷款"; }
        if ("泉壹贷".equals(str)){ return "||贷款"; }
        if ("大搜车秒贷".equals(str)){ return "||贷款"; }
        if ("立金所".equals(str)){ return "||贷款"; }
        if ("农金宝金融".equals(str)){ return "||贷款"; }
        if ("量子金融".equals(str)){ return "||贷款"; }
        if ("蜂鸟屋".equals(str)){ return "||贷款"; }
        if ("乐享宝".equals(str)){ return "||贷款"; }
        if ("万达普惠".equals(str)){ return "||贷款"; }
        if ("中潮金融P2P".equals(str)){ return "||贷款"; }
        if ("海河金融".equals(str)){ return "||贷款"; }
        if ("52短期贷电子签约".equals(str)){ return "||贷款"; }
        if ("港美股".equals(str)){ return "||贷款"; }
        if ("零浩电子合同".equals(str)){ return "||贷款"; }
        if ("标准签正式环境".equals(str)){ return "||贷款"; }
        if ("秒利宝".equals(str)){ return "||贷款"; }
        if ("正事贷".equals(str)){ return "||贷款"; }
        if ("小茗钱包".equals(str)){ return "||贷款"; }
        if ("e通宝".equals(str)){ return "||贷款"; }
        if ("德华小贷".equals(str)){ return "||贷款"; }
        if ("丹霞资本".equals(str)){ return "||贷款"; }
        if ("黄金钱庄".equals(str)){ return "||贷款"; }
        if ("小鹰互联认证".equals(str)){ return "||贷款"; }
        if ("SHCF".equals(str)){ return "||其他"; }
        if ("bjcs-web".equals(str)){ return "||其他"; }
        if ("石投金融".equals(str)){ return "||贷款"; }
        if ("益学电子签名".equals(str)){ return "||贷款"; }
        if ("沙包在线PC端".equals(str)){ return "||贷款"; }
        if ("好屋在线服务提醒".equals(str)){ return "||贷款"; }
        if ("大数金融印章".equals(str)){ return "||贷款"; }
        if ("利魔方".equals(str)){ return "||贷款"; }
        if ("汇邦小贷".equals(str)){ return "||贷款"; }
        if ("万商贷项目01签名".equals(str)){ return "||贷款"; }
        if ("得易贷".equals(str)){ return "||贷款"; }
        if ("铭捷财富".equals(str)){ return "||贷款"; }
        if ("聚业".equals(str)){ return "||贷款"; }
        if ("企乐贷".equals(str)){ return "||贷款"; }
        if ("百世优货".equals(str)){ return "||贷款"; }
        if ("国元网金电子签章".equals(str)){ return "||贷款"; }
        if ("掌财宝".equals(str)){ return "||贷款"; }
        if ("要你所想".equals(str)){ return "||贷款"; }
        if ("赊销宝".equals(str)){ return "||贷款"; }
        if ("兴易贷".equals(str)){ return "||贷款"; }
        if ("粤财汇P2P".equals(str)){ return "||贷款"; }
        if ("学生贷".equals(str)){ return "||贷款"; }
        if ("银狐财富".equals(str)){ return "||贷款"; }
        if ("城城理财".equals(str)){ return "||贷款"; }
        if ("浙农金服".equals(str)){ return "||贷款"; }
        if ("互汇盈".equals(str)){ return "||贷款"; }
        if ("第五创".equals(str)){ return "||贷款"; }
        if ("vipysdd".equals(str)){ return "||其他"; }
        if ("派派猪理财".equals(str)){ return "||贷款"; }
        if ("dreammove".equals(str)){ return "||其他"; }
        if ("春天金融".equals(str)){ return "||贷款"; }
        if ("快版律师".equals(str)){ return "||贷款"; }
        if ("华侨宝理财".equals(str)){ return "||贷款"; }
        if ("兴耀E签宝子系统".equals(str)){ return "||贷款"; }
        if ("途虎养车网".equals(str)){ return "||贷款"; }
        if ("金召金".equals(str)){ return "||贷款"; }
        if ("货运圈".equals(str)){ return "||贷款"; }
        if ("信义仓理财".equals(str)){ return "||贷款"; }
        if ("悦消费".equals(str)){ return "||贷款"; }
        if ("涌金盟".equals(str)){ return "||贷款"; }
        if ("贝兜金服".equals(str)){ return "||贷款"; }
        if ("小宝金服".equals(str)){ return "||贷款"; }
        if ("助借宝".equals(str)){ return "||贷款"; }
        if ("全球贷电子签章".equals(str)){ return "||贷款"; }
        if ("蜘蛛众筹".equals(str)){ return "||贷款"; }
        if ("慧易通".equals(str)){ return "||贷款"; }
        if ("初版".equals(str)){ return "||贷款"; }
        if ("三牛金服".equals(str)){ return "||贷款"; }
        if ("诚信贷".equals(str)){ return "||贷款"; }
        if ("房众金融".equals(str)){ return "||贷款"; }
        if ("深圳前海皓能互联网服务有限公司".equals(str)){ return "||贷款"; }
        if ("寓道".equals(str)){ return "||贷款"; }
        if ("qiantwo".equals(str)){ return "||其他"; }
        if ("四川森淼融联科技有限公司".equals(str)){ return "||贷款"; }
        if ("贷投乐电子合同".equals(str)){ return "||贷款"; }
        if ("三一金服".equals(str)){ return "||贷款"; }
        if ("洋萌电子签合同".equals(str)){ return "||贷款"; }
        if ("华云音乐".equals(str)){ return "||贷款"; }
        if ("喜氏电子票据交易撮合平台".equals(str)){ return "||贷款"; }
        if ("生息金服".equals(str)){ return "||贷款"; }
        if ("极信云科".equals(str)){ return "||贷款"; }
        return "";
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径

        Job job = Job.getInstance(conf, "EqianbaoMr");
        job.setJarByClass(EqianbaoMr.class);//主类
        //创建scan
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(args[0] + "!"));
        scan.setStopRow(Bytes.toBytes(args[0] + "~"));
        //可以指定查询某一列
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("ID"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("CERT_NO"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("type"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("project"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("Etime"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("evidenceId"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("GMT_CREATED"));
        scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("GMT_MODIFIED"));
        TableMapReduceUtil.initTableMapperJob("e_qianbao_data", scan, EqianbaoMr.MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("e_qianbao_data_1y_formal", EqianbaoMr.MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
