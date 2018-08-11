package yyyq.datasync;

import com.alibaba.druid.pool.DruidPooledPreparedStatement;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;
import yyyq.util.druidOrPool.DruidUtil;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class GxbDianshang2PhoenixThread {

    private static Connection incomeListConn;
    private static Connection behaviorInfoConn;
    private static Connection taobaoAddressListConn;
    private static Connection assetsInfoConn;
    private static Connection taobaoShopListConn;
    private static Connection baseInfoConn;
    private static Connection consumptionListConn;
    private static Connection repaymentListConn;

    static {
        incomeListConn = DruidUtil.getConn();
        behaviorInfoConn = DruidUtil.getConn();
        taobaoAddressListConn = DruidUtil.getConn();
        assetsInfoConn = DruidUtil.getConn();
        taobaoShopListConn = DruidUtil.getConn();
        baseInfoConn = DruidUtil.getConn();
        consumptionListConn = DruidUtil.getConn();
        repaymentListConn = DruidUtil.getConn();
    }

    public static void readFileByLines(String filePath, String voucher_no, String client_no, String cert_no) {
        File file = null;
        try {
            file = new File(filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (file != null) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"));
                String tempString = null;
                while ((tempString = reader.readLine()) != null) {
                    try {
                        JSONObject jobj = JSON.parseObject(tempString);
                        //  emay_basic
                        JSONObject incomeReport = (JSONObject) jobj.get("incomeReport");
                        JSONArray incomeList = incomeReport.getJSONArray("incomeList");
                        inserIncomeList(incomeList, cert_no, voucher_no, client_no);

                        JSONObject reportSummary = (JSONObject) jobj.get("reportSummary");
                        JSONObject behaviorInfo = (JSONObject) reportSummary.get("behaviorInfo");
                        insertBehaviorInfo(behaviorInfo, cert_no, voucher_no, client_no);

                        JSONArray taobaoAddressList = reportSummary.getJSONArray("taobaoAddressList");
                        insertTaobaoAddressList(taobaoAddressList, cert_no, voucher_no, client_no);

                        JSONObject assetsInfo = (JSONObject) reportSummary.get("assetsInfo");
                        insertAssetsInfo(assetsInfo, cert_no, voucher_no, client_no);

                        JSONArray taobaoShopList = reportSummary.getJSONArray("taobaoShopList");
                        insertTaobaoShopList(taobaoShopList, cert_no, voucher_no, client_no);

                        JSONObject baseInfo = (JSONObject) reportSummary.get("baseInfo");
                        insertBaseInfo(baseInfo, cert_no, voucher_no, client_no);

                        JSONObject expenditureReport = (JSONObject) jobj.get("expenditureReport");
                        JSONArray consumptionList = expenditureReport.getJSONArray("consumptionList");
                        insertConsumptionList(consumptionList, cert_no, voucher_no, client_no);

                        JSONArray repaymentList = expenditureReport.getJSONArray("repaymentList");
                        insertRepaymentList(repaymentList, cert_no, voucher_no, client_no);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                    }
                }
            }
        }
    }

    private static void insertRepaymentList(JSONArray repaymentList, String cert_no, String voucher_no, String client_no) {
        String sql = "upsert into \"gxb_repaymentList_test\" (\"ID\",\"jiebeiAmount\",\"creditCount\",\"huabeiAmount\",\"jiebeiCount\",\"creditAmount\"," +
                "\"month\",\"otherCount\",\"huabeiCount\",\"otherAmount\",\"client_no\",\"cert_no\",\"voucher_no\",\"loadtime\") " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
//        Connection repaymentListConn = null;
        try {
//            repaymentListConn = GetConnection.getPhoenixConn();
            DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) repaymentListConn.prepareStatement(sql);
//            PreparedStatement ps = repaymentListConn.prepareStatement(sql);
            int count = 0;
            for (Iterator iterator = repaymentList.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                String jiebeiAmount = job.getString("jiebeiAmount");
                String creditCount = job.getString("creditCount");
                String huabeiAmount = job.getString("huabeiAmount");
                String jiebeiCount = job.getString("jiebeiCount");
                String creditAmount = job.getString("creditAmount");
                String month = job.getString("month");
                String otherCount = job.getString("otherCount");
                String huabeiCount = job.getString("huabeiCount");
                String otherAmount = job.getString("otherAmount");

                ps.setString(1, client_no + "_" + cert_no + "_" + count);
                ps.setString(2,jiebeiAmount);
                ps.setString(3,creditCount);
                ps.setString(4,huabeiAmount);
                ps.setString(5,jiebeiCount);
                ps.setString(6,creditAmount);
                ps.setString(7,month);
                ps.setString(8,otherCount);
                ps.setString(9,huabeiCount);
                ps.setString(10,otherAmount);
                ps.setString(11,client_no);
                ps.setString(12,cert_no);
                ps.setString(13,voucher_no);
                ps.setString(14, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                ps.addBatch();
                count++;

                if (count % 100 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            repaymentListConn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            /*if (repaymentListConn != null){
                try {
                    repaymentListConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    private static void insertConsumptionList(JSONArray consumptionList, String cert_no, String voucher_no, String client_no) {
        String sql = "upsert into \"gxb_consumptionList_test\" (\"ID\", \"maxConsumeAmount\", \"gameRate\", \"totalConsumeAmount\", \"gameAmount\", " +
                "\"totalOutAmount\", \"maxRedpktOutAmount\", \"onlineShoppingCount\", \"lotteryRate\", \"redpktOutAmount\", \"totalConsumeCount\", " +
                "\"lotteryCount\", \"cateringAmount\", \"intimacyPayAmount\", \"transferOutAmount\", \"lotteryAmount\", \"redpktOutCount\", " +
                "\"lifepayAmount\", \"travelAmount\", \"onlineShoppingAmount\", \"travelCount\", \"maxTransferOutAmount\", \"month\", \"intimacyPayCount\", " +
                "\"cateringCount\", \"gameCount\", \"lifepayCount\", \"transferOutCount\", \"client_no\", \"cert_no\", \"voucher_no\", \"loadtime\") " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

//        Connection consumptionListConn = null;
        try {
//            consumptionListConn = GetConnection.getPhoenixConn();
            DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) consumptionListConn.prepareStatement(sql);
//            PreparedStatement ps = consumptionListConn.prepareStatement(sql);

            int count = 0;
            for (Iterator iterator = consumptionList.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                String maxConsumeAmount = job.getString("maxConsumeAmount");
                String gameRate = job.getString("gameRate");
                String totalConsumeAmount = job.getString("totalConsumeAmount");
                String gameAmount = job.getString("gameAmount");
                String totalOutAmount = job.getString("totalOutAmount");
                String maxRedpktOutAmount = job.getString("maxRedpktOutAmount");
                String onlineShoppingCount = job.getString("onlineShoppingCount");
                String lotteryRate = job.getString("lotteryRate");
                String redpktOutAmount = job.getString("redpktOutAmount");
                String totalConsumeCount = job.getString("totalConsumeCount");
                String lotteryCount = job.getString("lotteryCount");
                String cateringAmount = job.getString("cateringAmount");
                String intimacyPayAmount = job.getString("intimacyPayAmount");
                String transferOutAmount = job.getString("transferOutAmount");
                String lotteryAmount = job.getString("lotteryAmount");
                String redpktOutCount = job.getString("redpktOutCount");
                String lifepayAmount = job.getString("lifepayAmount");
                String travelAmount = job.getString("travelAmount");
                String onlineShoppingAmount = job.getString("onlineShoppingAmount");
                String travelCount = job.getString("travelCount");
                String maxTransferOutAmount = job.getString("maxTransferOutAmount");
                String month = job.getString("month");
                String intimacyPayCount = job.getString("intimacyPayCount");
                String cateringCount = job.getString("cateringCount");
                String gameCount = job.getString("gameCount");
                String lifepayCount = job.getString("lifepayCount");
                String transferOutCount = job.getString("transferOutCount");

                ps.setString(1, client_no + "_" + cert_no + "_" + count);
                ps.setString(2,maxConsumeAmount);
                ps.setString(3,gameRate);
                ps.setString(4,totalConsumeAmount);
                ps.setString(5,gameAmount);
                ps.setString(6,totalOutAmount);
                ps.setString(7,maxRedpktOutAmount);
                ps.setString(8,onlineShoppingCount);
                ps.setString(9,lotteryRate);
                ps.setString(10,redpktOutAmount);
                ps.setString(11,totalConsumeCount);
                ps.setString(12,lotteryCount);
                ps.setString(13,cateringAmount);
                ps.setString(14,intimacyPayAmount);
                ps.setString(15,transferOutAmount);
                ps.setString(16,lotteryAmount);
                ps.setString(17,redpktOutCount);
                ps.setString(18,lifepayAmount);
                ps.setString(19,travelAmount);
                ps.setString(20,onlineShoppingAmount);
                ps.setString(21,travelCount);
                ps.setString(22,maxTransferOutAmount);
                ps.setString(23,month);
                ps.setString(24,intimacyPayCount);
                ps.setString(25,cateringCount);
                ps.setString(26,gameCount);
                ps.setString(27,lifepayCount);
                ps.setString(28,transferOutCount);
                ps.setString(29,client_no);
                ps.setString(30,cert_no);
                ps.setString(31,voucher_no);
                ps.setString(32, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                ps.addBatch();
                count++;

                if (count % 100 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            consumptionListConn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            /*if (consumptionListConn != null){
                try {
                    consumptionListConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    private static void insertBaseInfo(JSONObject job, String cert_no, String voucher_no, String client_no) {
        String sql = "upsert into \"gxb_baseInfo_test\" ( \"ID\",\"creditLevelAsBuyer\",\"idCard\",\"phone\",\"intimacyPayList\",\"totalIncomeOf6m\"," +
                "\"fundTranseOf6m\",\"status\",\"registerDate\",\"isVarified\",\"taobaoAccount\",\"totalExpenditureOf6m\",\"alipayEmail\"," +
                "\"alipayAccountType\",\"name\",\"alipayAccount\",\"totalRepayOf6m\",\"client_no\",\"cert_no\",\"voucher_no\",\"loadtime\") " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

//        Connection baseInfoConn = null;
        try {
//            baseInfoConn = GetConnection.getPhoenixConn();
            DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) baseInfoConn.prepareStatement(sql);
//            PreparedStatement ps = baseInfoConn.prepareStatement(sql);

            String creditLevelAsBuyer = job.getString("creditLevelAsBuyer");
            String idCard = job.getString("idCard");
            String phone = job.getString("phone");
            JSONArray intimacyPayList = job.getJSONArray("intimacyPayList");
            String intimacyPayList_s = "0";
            try {
                if (intimacyPayList.size() > 0 ){
                    StringBuffer sb = new StringBuffer();
                    for (Iterator iterator = intimacyPayList.iterator(); iterator.hasNext(); ) {
                        JSONObject job_ = (JSONObject) iterator.next();
                        String intimacyPayName = job_.getString("intimacyPayName");
                        String intimacyPayAccount = job_.getString("intimacyPayAccount");
                        if (intimacyPayName != null && !"".equals(intimacyPayName) && !"null".equals(intimacyPayName)){
                            sb.append(intimacyPayName+":");
                            sb.append(intimacyPayAccount+",");
                        }
                    }
                    intimacyPayList_s = sb.toString();
                    if (!"".equals(intimacyPayList_s)){
                        intimacyPayList_s = intimacyPayList_s.substring(0, intimacyPayList_s.length()-1);
                    }else {
                        intimacyPayList_s = "0";
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            String totalIncomeOf6m = job.getString("totalIncomeOf6m");
            String fundTranseOf6m = job.getString("fundTranseOf6m");
            String status = job.getString("status");
            String registerDate = job.getString("registerDate");
            String isVarified = job.getString("isVarified");
            String taobaoAccount = job.getString("taobaoAccount");
            String totalExpenditureOf6m = job.getString("totalExpenditureOf6m");
            String alipayEmail = job.getString("alipayEmail");
            String alipayAccountType = job.getString("alipayAccountType");
            String name = job.getString("name");
            String alipayAccount = job.getString("alipayAccount");
            String totalRepayOf6m = job.getString("totalRepayOf6m");

            ps.setString(1,client_no + "_" + cert_no);
            ps.setString(2,creditLevelAsBuyer);
            ps.setString(3,idCard);
            ps.setString(4,phone);
            ps.setString(5,intimacyPayList_s);
            ps.setString(6,totalIncomeOf6m);
            ps.setString(7,fundTranseOf6m);
            ps.setString(8,status);
            ps.setString(9,registerDate);
            ps.setString(10,isVarified);
            ps.setString(11,taobaoAccount);
            ps.setString(12,totalExpenditureOf6m);
            ps.setString(13,alipayEmail);
            ps.setString(14,alipayAccountType);
            ps.setString(15,name);
            ps.setString(16,alipayAccount);
            ps.setString(17,totalRepayOf6m);
            ps.setString(18,client_no);
            ps.setString(19,cert_no);
            ps.setString(20,voucher_no);
            ps.setString(21, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
            ps.executeUpdate();
            baseInfoConn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            /*if (baseInfoConn != null){
                try {
                    baseInfoConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    private static void insertTaobaoShopList(JSONArray taobaoShopList, String cert_no, String voucher_no, String client_no) {
        String sql = "upsert into \"gxb_taobaoShopList_test\" (\"ID\",\"amount\",\"count\",\"shopName\",\"shopUrl\",\"shopNick\",\"client_no\"," +
                "\"cert_no\",\"voucher_no\",\"loadtime\") values (?,?,?,?,?,?,?,?,?,?) ";

//        Connection taobaoShopListConn = null;
        try {
//            taobaoShopListConn = GetConnection.getPhoenixConn();
            DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) taobaoShopListConn.prepareStatement(sql);
//            PreparedStatement ps = taobaoShopListConn.prepareStatement(sql);

            int count = 0;
            for (Iterator iterator = taobaoShopList.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                String amount = job.getString("amount");
                String count_ = job.getString("count");
                String shopName = job.getString("shopName");
                String shopUrl = job.getString("shopUrl");
                String shopNick = job.getString("shopNick");

                ps.setString(1, client_no + "_" + cert_no + "_" + count);
                ps.setString(2,amount);
                ps.setString(3,count_);
                ps.setString(4,shopName);
                ps.setString(5,shopUrl);
                ps.setString(6,shopNick);
                ps.setString(7, client_no);
                ps.setString(8, cert_no);
                ps.setString(9, voucher_no);
                ps.setString(10, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                ps.addBatch();
                count++;

                if (count % 100 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            taobaoShopListConn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            /*if (taobaoShopListConn != null){
                try {
                    taobaoShopListConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    private static void insertAssetsInfo(JSONObject job, String cert_no, String voucher_no, String client_no) {
        String sql = "upsert into \"gxb_assetsInfo_test\" ( \"ID\",\"huabeiOverdueBillCnt\",\"huabeiStatus\",\"huabeiPayDay\",\"huabeiCurrentMonthPayment\"," +
                "\"huabeiPenaltyAmount\",\"huabeiAmount\",\"alipayBalance\",\"jiebeiAmount\",\"huabeiBalance\",\"huabeiNextMonthPayment\",\"yuebaoBalance\"," +
                "\"jiebeiBalance\",\"huabeiOriginalAmount\",\"huabeiOverdueDays\",\"client_no\",\"cert_no\",\"voucher_no\",\"loadtime\") " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

//        Connection assetsInfoConn = null;
        try {
//            assetsInfoConn = GetConnection.getPhoenixConn();
            DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) assetsInfoConn.prepareStatement(sql);
//            PreparedStatement ps = assetsInfoConn.prepareStatement(sql);

            String huabeiOverdueBillCnt = job.getString("huabeiOverdueBillCnt");
            String huabeiStatus = job.getString("huabeiStatus");
            String huabeiPayDay = job.getString("huabeiPayDay");
            String huabeiCurrentMonthPayment = job.getString("huabeiCurrentMonthPayment");
            String huabeiPenaltyAmount = job.getString("huabeiPenaltyAmount");
            String huabeiAmount = job.getString("huabeiAmount");
            String alipayBalance = job.getString("alipayBalance");
            String jiebeiAmount = job.getString("jiebeiAmount");
            String huabeiBalance = job.getString("huabeiBalance");
            String huabeiNextMonthPayment = job.getString("huabeiNextMonthPayment");
            String yuebaoBalance = job.getString("yuebaoBalance");
            String jiebeiBalance = job.getString("jiebeiBalance");
            String huabeiOriginalAmount = job.getString("huabeiOriginalAmount");
            String huabeiOverdueDays = job.getString("huabeiOverdueDays");
            ps.setString(1,client_no + "_" + cert_no);
            ps.setString(2,huabeiOverdueBillCnt);
            ps.setString(3,huabeiStatus);
            ps.setString(4,huabeiPayDay);
            ps.setString(5,huabeiCurrentMonthPayment);
            ps.setString(6,huabeiPenaltyAmount);
            ps.setString(7,huabeiAmount);
            ps.setString(8,alipayBalance);
            ps.setString(9,jiebeiAmount);
            ps.setString(10,huabeiBalance);
            ps.setString(11,huabeiNextMonthPayment);
            ps.setString(12,yuebaoBalance);
            ps.setString(13,jiebeiBalance);
            ps.setString(14,huabeiOriginalAmount);
            ps.setString(15,huabeiOverdueDays);
            ps.setString(16,client_no);
            ps.setString(17,cert_no);
            ps.setString(18,voucher_no);
            ps.setString(19, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
            ps.executeUpdate();
            assetsInfoConn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            /*if (assetsInfoConn != null){
                try {
                    assetsInfoConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    private static void insertTaobaoAddressList(JSONArray taobaoAddressList, String cert_no, String voucher_no, String client_no) {
        String sql = "upsert into \"gxb_taobaoAddressList_test\" (\"ID\", \"address\", \"tradeCount\", \"tradeCountOf3m\", \"postCode\", \"receiveName\", " +
                "\"telNumber\", \"client_no\", \"cert_no\", \"voucher_no\", \"loadtime\") values (?,?,?,?,?,?,?,?,?,?,?) ";

//        Connection taobaoAddressListConn = null;
        try {
//            taobaoAddressListConn = GetConnection.getPhoenixConn();
            DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) taobaoAddressListConn.prepareStatement(sql);
//            PreparedStatement ps = taobaoAddressListConn.prepareStatement(sql);

            int count = 0;
            for (Iterator iterator = taobaoAddressList.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                String address = job.getString("address");
                String tradeCount = job.getString("tradeCount");
                String tradeCountOf3m =job.getString("tradeCountOf3m");
                String postCode = job.getString("postCode");
                String receiveName = job.getString("receiveName");
                String telNumber = job.getString("telNumber");

                ps.setString(1, client_no + "_" + cert_no + "_" + count);
                ps.setString(2, address);
                ps.setString(3, tradeCount);
                ps.setString(4, tradeCountOf3m);
                ps.setString(5, postCode);
                ps.setString(6, receiveName);
                ps.setString(7, telNumber);
                ps.setString(8, client_no);
                ps.setString(9, cert_no);
                ps.setString(10, voucher_no);
                ps.setString(11, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                ps.addBatch();
                count++;

                if (count % 100 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            taobaoAddressListConn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            /*if (taobaoAddressListConn != null){
                try {
                    taobaoAddressListConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    private static void insertBehaviorInfo(JSONObject job, String cert_no, String voucher_no, String client_no) {
        String sql = "upsert into \"gxb_behaviorInfo_test\" (\"ID\", \"drugCount\", \"lieCount\", \"gambleCount\", \"highRiskAreaCount\", \"sensitiveCount\", " +
                "\"otherCount\", \"client_no\", \"cert_no\", \"voucher_no\", \"loadtime\") values (?,?,?,?,?,?,?,?,?,?,?) ";

//        Connection behaviorInfoConn = null;
        try {
//            behaviorInfoConn = GetConnection.getPhoenixConn();
            DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) behaviorInfoConn.prepareStatement(sql);
//            PreparedStatement ps = behaviorInfoConn.prepareStatement(sql);

            String drugCount = job.getString("drugCount");
            String lieCount = job.getString("lieCount");
            String gambleCount = job.getString("gambleCount");
            String highRiskAreaCount = job.getString("highRiskAreaCount");
            String sensitiveCount = job.getString("sensitiveCount");
            String otherCount = job.getString("otherCount");
            ps.setString(1, client_no + "_" + cert_no);
            ps.setString(2, drugCount);
            ps.setString(3, lieCount);
            ps.setString(4, gambleCount);
            ps.setString(5, highRiskAreaCount);
            ps.setString(6, sensitiveCount);
            ps.setString(7, otherCount);
            ps.setString(8, client_no);
            ps.setString(9, cert_no);
            ps.setString(10, voucher_no);
            ps.setString(11, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
//            ps.addBatch();
//            ps.executeBatch();
            ps.executeUpdate();
            behaviorInfoConn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            /*if (behaviorInfoConn != null){
                try {
                    behaviorInfoConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    private static void inserIncomeList(JSONArray incomeList, String cert_no, String voucher_no, String client_no) throws Exception {
        String sql = "upsert into \"gxb_incomeList_test\" (\"ID\", \"yuebaoAmount\", \"refundCount\", \"maxRefundAmount\", \"maxTransferInAmount\", \"transferInCount\", " +
                "\"transferInAmount\", \"redpktAmount\", \"refundAmount\", \"month\", \"jiebeiLoanAmount\", \"totalInAmount\", \"redpktCount\", " +
                "\"jiebeiLoanCount\", \"maxRedpktAmount\", \"client_no\", \"cert_no\", \"voucher_no\", \"loadtime\") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

//        Connection incomeListConn = null;
        try {
//            incomeListConn = GetConnection.getPhoenixConn();
            DruidPooledPreparedStatement ps = (DruidPooledPreparedStatement) incomeListConn.prepareStatement(sql);
//            PreparedStatement ps = incomeListConn.prepareStatement(sql);
            int count = 0;
            for (Iterator iterator = incomeList.iterator(); iterator.hasNext(); ) {
                JSONObject job = (JSONObject) iterator.next();
                String yuebaoAmount = job.getString("yuebaoAmount");
                String refundCount = job.getString("refundCount");
                String maxRefundAmount = job.getString("maxRefundAmount");
                String maxTransferInAmount = job.getString("maxTransferInAmount");
                String transferInCount = job.getString("transferInCount");
                String transferInAmount = job.getString("transferInAmount");
                String redpktAmount = job.getString("redpktAmount");
                String refundAmount = job.getString("refundAmount");
                String month = job.getString("month");
                String jiebeiLoanAmount = job.getString("jiebeiLoanAmount");
                String totalInAmount = job.getString("totalInAmount");
                String redpktCount = job.getString("redpktCount");
                String jiebeiLoanCount = job.getString("jiebeiLoanCount");
                String maxRedpktAmount = job.getString("maxRedpktAmount");
                ps.setString(1, client_no + "_" + cert_no + "_" + count);
                ps.setString(2, yuebaoAmount);
                ps.setString(3, refundCount);
                ps.setString(4, maxRefundAmount);
                ps.setString(5, maxTransferInAmount);
                ps.setString(6, transferInCount);
                ps.setString(7, transferInAmount);
                ps.setString(8, redpktAmount);
                ps.setString(9, refundAmount);
                ps.setString(10, month);
                ps.setString(11, jiebeiLoanAmount);
                ps.setString(12, totalInAmount);
                ps.setString(13, redpktCount);
                ps.setString(14, jiebeiLoanCount);
                ps.setString(15, maxRedpktAmount);
                ps.setString(16, client_no);
                ps.setString(17, cert_no);
                ps.setString(18, voucher_no);
                ps.setString(19, DateUtil.nowString("yyyy-MM-dd HH:mm:ss,sss"));
                ps.addBatch();
                count++;

                if (count % 100 == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            incomeListConn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            /*if (incomeListConn != null){
                try {
                    incomeListConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }


    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        int files = 0;
        Connection conn = GetConnection.getConn_Cdsp_Riskcore();
        Connection conn_ = GetConnection.getConn_Bops_Sales();
        String sql = "";
        sql = "SELECT VOUCHER_NO,FILE_PATH FROM cdsp_hulu_access n WHERE QUERY_TYPE = 'GXB_ECOMMERCE_REPORT' " +
                "AND n.GMT_CREATED BETWEEN '" + args[0] + "' AND '" + args[1] + "' ";
        PreparedStatement pps;
        try {
            pps = conn.prepareStatement(sql);
            ResultSet rs = pps.executeQuery();
            while (rs.next()) {
                String voucher_no = rs.getString("VOUCHER_NO");     // 授信编号
                String filePath = rs.getString("FILE_PATH");        // 文件路径
                String sql_ = "SELECT client_no FROM bops.bops_loan_request r WHERE REQ_no = '" + voucher_no + "'; ";
                pps = conn_.prepareStatement(sql_);
                ResultSet rs_ = pps.executeQuery();
                if (!rs_.next()){
                    System.out.println("===============client_no-null=============="+voucher_no+"======1"+rs_.next());
                    String sql_r = "SELECT client_no FROM bops.bops_credit_request r WHERE credit_no = '" + voucher_no + "'; ";
                    pps = conn_.prepareStatement(sql_r);
                    rs_ = pps.executeQuery();
                }
                while (rs_.next()) {
                    String client_no = rs_.getString("client_no");   // 客户编号
                    String _sql = "SELECT CERT_NO FROM sales.sale_user  WHERE client_no = '" + client_no + "'; ";
                    pps = conn_.prepareStatement(_sql);
                    ResultSet _rs = pps.executeQuery();
                    while (_rs.next()) {
                        String cert_no = _rs.getString("CERT_NO");    // 身份证号
                        filePath = "D:\\tmp\\0009500108000dK1z3tycMD1lLyaFIP1.json";
                        readFileByLines(filePath, voucher_no, client_no, cert_no);
                        System.out.println(DateUtil.nowString() + " ==GxbDianshang=files====" + ++files);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.close();
            conn_.close();
            incomeListConn.close();
            behaviorInfoConn.close();
            taobaoAddressListConn.close();
            assetsInfoConn.close();
            taobaoShopListConn.close();
            baseInfoConn.close();
            consumptionListConn.close();
            repaymentListConn.close();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString() + " GxbDianshang2Phoenix导入耗时为： " + (endtime - starttime));
    }


}
