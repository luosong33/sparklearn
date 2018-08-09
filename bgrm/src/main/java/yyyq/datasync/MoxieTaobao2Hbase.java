package yyyq.datasync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.DateUtil;
import yyyq.util.GetConnection;
import yyyq.util.IdWorker;

import java.io.*;
import java.sql.*;
import java.util.*;

public class MoxieTaobao2Hbase {

    private static String nowDate = DateUtil.nowString();
    //  淘宝用户基本信息
    private static void insertIntoHbase_userinfo(String voucherNo, JSONObject job) {

        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("upsert  into \"bops_client_taobaobasic_userinfo\" (\"ROW\", \"mapping_id\", \"nick\", \"real_name\", \"phone_number\", \"email\", " +
                    "\"vip_level\", \"vip_count\", \"weibo_account\", \"weibo_nick\", \"pic\", \"alipay_account\", \"loadtime\") values (?,?,?,?,?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            stmt.setString(1, voucherNo);
            stmt.setString(2, String.valueOf(job.get("mapping_id")));
            stmt.setString(3, String.valueOf(job.get("nick")));
            stmt.setString(4, String.valueOf(job.get("real_name")));
            stmt.setString(5, String.valueOf(job.get("phone_number")));
            stmt.setString(6, String.valueOf(job.get("email")));
            stmt.setString(7, String.valueOf(job.get("vip_level")));
            stmt.setString(8, String.valueOf(job.get("vip_count")));
            stmt.setString(9, String.valueOf(job.get("weibo_account")));
            stmt.setString(10, String.valueOf(job.get("weibo_nick")));
            stmt.setString(11, String.valueOf(job.get("pic")));
            stmt.setString(12, String.valueOf(job.get("alipay_account")));
            stmt.setString(13, String.valueOf(nowDate));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            stmt.addBatch();
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //  淘宝最近的送货地址
    private static void insertIntoHbase_recentdeliveraddress(String voucherNo, JSONArray jarr) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("upsert  into \"bops_client_taobaobasic_recentdeliveraddress\" (\"ROW\", \"province\", \"city\", \"trade_id\", \"trade_createtime\", " +
                    "\"actual_fee\", \"deliver_name\", \"deliver_mobilephone\", \"deliver_fixedphone\", \"deliver_address\", \"deliver_postcode\", \"invoice_name\", \"voucher_no\", \"loadtime\") " +
                    "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            try {
                stmt.setString(1, voucherNo + "_" + String.valueOf(job.get("trade_id")));
                stmt.setString(2, String.valueOf(job.get("province")));
                stmt.setString(3, String.valueOf(job.get("city")));
                stmt.setString(4, String.valueOf(job.get("trade_id")));
                stmt.setString(5, String.valueOf(job.get("trade_createtime")));
                stmt.setString(6, String.valueOf(job.get("actual_fee")));
                stmt.setString(7, String.valueOf(job.get("deliver_name")));
                stmt.setString(8, String.valueOf(job.get("deliver_mobilephone")));
                stmt.setString(9, String.valueOf(job.get("deliver_fixedphone")));
                stmt.setString(10, String.valueOf(job.get("deliver_address")));
                stmt.setString(11, String.valueOf(job.get("deliver_postcode")));
                stmt.setString(12, String.valueOf(job.get("invoice_name")));
                stmt.setString(13, voucherNo);
                stmt.setString(14, String.valueOf(nowDate));
                stmt.addBatch();
                count++;

                if (count % 1000 == 0) {
                    try {
                        stmt.executeBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        try {
            stmt.executeBatch();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //  淘宝送货地址
    private static void insertIntoHbase_deliveraddress(String voucherNo, JSONArray jarr) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("upsert  into \"bops_client_taobaobasic_deliveraddress\" (\"ROW\", \"name\", \"address\", \"province\", \"city\", \"default\", " +
                    "\"mapping_id\", \"full_address\", \"zip_code\", \"phone_no\", \"voucher_no\", \"loadtime\") values (?,?,?,?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            try {
                IdWorker idWorker = new IdWorker(5);
                long nextId = idWorker.nextId();
                stmt.setString(1, voucherNo + "_" + nextId);
                stmt.setString(2, String.valueOf(job.get("name")));
                stmt.setString(3, String.valueOf(job.get("address")));
                stmt.setString(4, String.valueOf(job.get("province")));
                stmt.setString(5, String.valueOf(job.get("city")));
                stmt.setString(6, String.valueOf(job.get("default")));
                stmt.setString(7, String.valueOf(job.get("mapping_id")));
                stmt.setString(8, String.valueOf(job.get("full_address")));
                stmt.setString(9, String.valueOf(job.get("zip_code")));
                stmt.setString(10, String.valueOf(job.get("phone_no")));
                stmt.setString(11, voucherNo);
                stmt.setString(12, String.valueOf(nowDate));
                stmt.addBatch();
                count++;
            } catch (SQLException e) {
                e.printStackTrace();
            }

            if (count % 1000 == 0) {
                try {
                    stmt.executeBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            stmt.executeBatch();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //  淘宝交易细节店铺信息
    private static void insertIntoHbase_tradedetails(String voucherNo, JSONArray jarr) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("upsert  into \"bops_client_taobaobasic_tradedetails\" (\"ROW\", \"mapping_id\", \"trade_id\", \"trade_status\", \"trade_createtime\", " +
                    "\"actual_fee\", \"seller_id\", \"seller_nick\", \"seller_shopname\", \"trade_text\", \"deliver_name\", \"deliver_mobilephone\", \"deliver_fixedphone\", " +
                    "\"deliver_address\", \"deliver_postcode\", \"deliver_fulladdress\", \"invoice_name\", \"voucher_no\", \"loadtime\") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            try {
                stmt.setString(1, voucherNo + "_" + String.valueOf(job.get("trade_id")));
                stmt.setString(2, String.valueOf(job.get("mapping_id")));
                stmt.setString(3, String.valueOf(job.get("trade_id")));
                stmt.setString(4, String.valueOf(job.get("trade_status")));
                stmt.setString(5, String.valueOf(job.get("trade_createtime")));
                stmt.setString(6, String.valueOf(job.get("actual_fee")));
                stmt.setString(7, String.valueOf(job.get("seller_id")));
                stmt.setString(8, String.valueOf(job.get("seller_nick")));
                stmt.setString(9, String.valueOf(job.get("seller_shopname")));
                stmt.setString(10, String.valueOf(job.get("trade_text")));
                stmt.setString(11, String.valueOf(job.get("deliver_name")));
                stmt.setString(12, String.valueOf(job.get("deliver_mobilephone")));
                stmt.setString(13, String.valueOf(job.get("deliver_fixedphone")));
                stmt.setString(14, String.valueOf(job.get("deliver_address")));
                stmt.setString(15, String.valueOf(job.get("deliver_postcode")));
                stmt.setString(16, String.valueOf(job.get("deliver_fulladdress")));
                stmt.setString(17, String.valueOf(job.get("invoice_name")));
                stmt.setString(18, voucherNo);
                stmt.setString(19, String.valueOf(nowDate));
                stmt.addBatch();
                count++;
            } catch (SQLException e) {
                e.printStackTrace();
            }

            if (count % 1000 == 0) {
                try {
                    stmt.executeBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            stmt.executeBatch();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //  淘宝交易细节商品信息
    private static void insertIntoHbase_suborders(String voucherNo, JSONArray jarr) {
        PreparedStatement stmt = null;
        Connection con = GetConnection.getPhoenixConn();
        try {
            con.setAutoCommit(false);
            stmt = con.prepareStatement("upsert  into \"bops_client_taobaobasic_suborders\" (\"ROW\", \"quantity\", \"mapping_id\", \"trade_id\", \"item_id\", \"item_url\", " +
                    "\"item_pic\", \"item_name\", \"original\", \"real_total\", \"voucher_no\", \"loadtime\") values (?,?,?,?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        int count = 0;
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            JSONArray subordersJarr = job.getJSONArray("sub_orders");

            for (Iterator iterator_ = subordersJarr.iterator(); iterator_.hasNext(); ) {
                JSONObject job_ = (JSONObject) iterator_.next();
                try {
                    stmt.setString(1, voucherNo + "_" + String.valueOf(job_.get("trade_id")));
                    stmt.setString(2, String.valueOf(job_.get("quantity")));
                    stmt.setString(3, String.valueOf(job_.get("mapping_id")));
                    stmt.setString(4, String.valueOf(job_.get("trade_id")));
                    stmt.setString(5, String.valueOf(job_.get("item_id")));
                    stmt.setString(6, String.valueOf(job_.get("item_url")));
                    stmt.setString(7, String.valueOf(job_.get("item_pic")));
                    stmt.setString(8, String.valueOf(job_.get("item_name")));
                    stmt.setString(9, String.valueOf(job_.get("original")));
                    stmt.setString(10, String.valueOf(job_.get("real_total")));
                    stmt.setString(11, voucherNo);
                    stmt.setString(12, String.valueOf(nowDate));
                    stmt.addBatch();
                    count++;

                    if (count % 1000 == 0) {
                        try {
                            stmt.executeBatch();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }

        try {
            stmt.executeBatch();
            con.commit();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void readFileByLines(String voucherNo, String fileName) {
        File file = null;
        try {
//            SshUtil.getContext("192.168.15.196","root","yinghuo#123", fileName);
//            fileName = "d:/tmp/b19d324a-e344-11e7-a7af-00163e004572.json";
            file = new File(fileName);
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
                        //  淘宝用户基本信息
                        JSONObject userinfoJson = (JSONObject) jobj.get("userinfo");
//                        insertIntoHbase_userinfo(voucherNo, userinfoJson);
                        //  淘宝最近的送货地址
                        JSONArray recentdeliveraddressJarr = jobj.getJSONArray("recentdeliveraddress");  //  数组
//                        insertIntoHbase_recentdeliveraddress(voucherNo, recentdeliveraddressJarr);
                        //  淘宝送货地址
                        JSONArray deliveraddressJarr = jobj.getJSONArray("deliveraddress");  //  数组
//                        insertIntoHbase_deliveraddress(voucherNo, deliveraddressJarr);
                        //  淘宝交易细节店铺信息
                        JSONObject tradedetailsJson = (JSONObject) jobj.get("tradedetails");
                        JSONArray tradedetailsJarr = tradedetailsJson.getJSONArray("tradedetails");
//                        insertIntoHbase_tradedetails(voucherNo, tradedetailsJarr);
                        //  淘宝交易细节商品信息
                        insertIntoHbase_suborders(voucherNo, tradedetailsJarr);
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

    public static void main(String[] args) throws SQLException {
        long starttime = System.currentTimeMillis();
        int files = 0;
        Connection conn = GetConnection.getConn_Cdsp_Riskcore();
        String sql = "";
        sql = "select VOUCHER_NO,FILE_PATH from cdsp_hulu_access where QUERY_TYPE = 'MOXIE_TAOBAO_BASIC' and  file_path IS NOT NULL "
        +"and GMT_CREATED >= '"+args[0]+"' " + " and GMT_CREATED < '"+args[1]+"'";  //  摩羯电商数据  2017-09-06 00:00:00
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String voucher = rs.getString("VOUCHER_NO");  //  葫芦、摩羯
                String filePath = rs.getString("FILE_PATH");
                readFileByLines(voucher, filePath);
                System.out.println(DateUtil.nowString()+" ==dianshang_basic=files====" + ++files + "===path===" + filePath);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            conn.close();
        }
        long endtime = System.currentTimeMillis();
        System.out.println(DateUtil.nowString()+" 电商basic导入耗时为： " + (endtime - starttime));
    }


}
