package yyyq.export;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.ReadWriteUtil;

import java.io.*;
import java.util.Iterator;

public class ExportData_ {

    //  淘宝用户基本信息
    private static void insertIntoHbase_userinfo(JSONObject job) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("mapping_id" + "\t" + "nick" + "\t" + "real_name" + "\t" + "phone_number" + "\t" + "email" + "\t" + "vip_level" + "\t" + "vip_count" + "\t" + "weibo_account" + "\t"
                + "weibo_nick" + "\t" + "pic" + "\t" + "alipay_account" + "\r\n");
        stringBuffer.append(
                String.valueOf(job.get("mapping_id")) + "\t" +
                        String.valueOf(job.get("nick")) + "\t" +
                        String.valueOf(job.get("real_name")) + "\t" +
                        String.valueOf(job.get("phone_number")) + "\t" +
                        String.valueOf(job.get("email")) + "\t" +
                        String.valueOf(job.get("vip_level")) + "\t" +
                        String.valueOf(job.get("vip_count")) + "\t" +
                        String.valueOf(job.get("weibo_account")) + "\t" +
                        String.valueOf(job.get("weibo_nick")) + "\t" +
                        String.valueOf(job.get("pic")) + "\t" +
                        String.valueOf(job.get("alipay_account")) + "\r\n");
        String s = stringBuffer.toString();
        ReadWriteUtil.write("E:\\ls\\1215.txt", s);
    }

    //  淘宝最近的送货地址
    private static void insertIntoHbase_recentdeliveraddress(JSONArray jarr) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("province" + "\t" + "city" + "\t" + "trade_id" + "\t" + "trade_createtime" + "\t" + "actual_fee" + "\t" + "deliver_name" + "\t" + "deliver_mobilephone" + "\t"
                + "deliver_fixedphone" + "\t" + "deliver_address" + "\t" + "deliver_postcode" + "\t" + "invoice_name" + "\r\n");
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            stringBuffer.append(
                    String.valueOf(job.get("province")) + "\t" +
                            String.valueOf(job.get("city")) + "\t" +
                            String.valueOf(job.get("trade_id")) + "\t" +
                            String.valueOf(job.get("trade_createtime")) + "\t" +
                            String.valueOf(job.get("actual_fee")) + "\t" +
                            String.valueOf(job.get("deliver_name")) + "\t" +
                            String.valueOf(job.get("deliver_mobilephone")) + "\t" +
                            String.valueOf(job.get("deliver_fixedphone")) + "\t" +
                            String.valueOf(job.get("deliver_address")) + "\t" +
                            String.valueOf(job.get("deliver_postcode")) + "\t" +
                            String.valueOf(job.get("invoice_name")) + "\r\n");
        }
        String s = stringBuffer.toString();
        ReadWriteUtil.write("E:\\ls\\1215.txt", s);
    }

    //  淘宝送货地址
    private static void insertIntoHbase_deliveraddress(JSONArray jarr) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("name" + "\t" + "address" + "\t" + "province" + "\t" + "city" + "\t" + "default" + "\t" + "mapping_id" + "\t" + "full_address" + "\t" + "zip_code" + "\t" + "phone_no" + "\r\n");
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            stringBuffer.append(
                    String.valueOf(job.get("name")) + "\t" +
                            String.valueOf(job.get("address")) + "\t" +
                            String.valueOf(job.get("province")) + "\t" +
                            String.valueOf(job.get("city")) + "\t" +
                            String.valueOf(job.get("default")) + "\t" +
                            String.valueOf(job.get("mapping_id")) + "\t" +
                            String.valueOf(job.get("full_address")) + "\t" +
                            String.valueOf(job.get("zip_code")) + "\t" +
                            String.valueOf(job.get("phone_no")) + "\r\n");
        }
        String s = stringBuffer.toString();
        ReadWriteUtil.write("E:\\ls\\1215.txt", s);
    }

    //  淘宝交易细节店铺信息
    private static void insertIntoHbase_tradedetails(JSONArray jarr) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("mapping_id" + "\t" + "trade_id" + "\t" + "trade_status" + "\t" + "trade_createtime" + "\t" + "actual_fee" + "\t" + "seller_id" + "\t" + "seller_nick" + "\t" + "seller_shopname"
                + "\t" + "trade_text" + "\t" + "deliver_name" + "\t" + "deliver_mobilephone" + "\t" + "deliver_fixedphone" + "\t" + "deliver_address" + "\t" + "deliver_postcode" + "\t" + "deliver_fulladdress"
                + "\t" + "invoice_name" + "\r\n");
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            stringBuffer.append(
                    String.valueOf(job.get("mapping_id")) + "\t" +
                            String.valueOf(job.get("trade_id")) + "\t" +
                            String.valueOf(job.get("trade_status")) + "\t" +
                            String.valueOf(job.get("trade_createtime")) + "\t" +
                            String.valueOf(job.get("actual_fee")) + "\t" +
                            String.valueOf(job.get("seller_id")) + "\t" +
                            String.valueOf(job.get("seller_nick")) + "\t" +
                            String.valueOf(job.get("seller_shopname")) + "\t" +
                            String.valueOf(job.get("trade_text")) + "\t" +
                            String.valueOf(job.get("deliver_name")) + "\t" +
                            String.valueOf(job.get("deliver_mobilephone")) + "\t" +
                            String.valueOf(job.get("deliver_fixedphone")) + "\t" +
                            String.valueOf(job.get("deliver_address")) + "\t" +
                            String.valueOf(job.get("deliver_postcode")) + "\t" +
                            String.valueOf(job.get("deliver_fulladdress")) + "\t" +
                            String.valueOf(job.get("invoice_name")) + "\r\n");
        }
        String s = stringBuffer.toString();
        ReadWriteUtil.write("E:\\ls\\1215.txt", s);
    }

    //  淘宝交易细节商品信息
    private static void insertIntoHbase_suborders(JSONArray jarr) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("trade_id" + "\t" + "quantity" + "\t" + "mapping_id" + "\t" + "trade_id" + "\t" + "item_id" + "\t" + "item_url" + "\t" + "item_pic" + "\t" + "item_name" + "\t" + "original" + "\t"
                + "real_total" + "\r\n");
        for (Iterator iterator = jarr.iterator(); iterator.hasNext(); ) {
            JSONObject job = (JSONObject) iterator.next();
            JSONArray subordersJarr = job.getJSONArray("sub_orders");

            for (Iterator iterator_ = subordersJarr.iterator(); iterator_.hasNext(); ) {
                JSONObject job_ = (JSONObject) iterator_.next();
                stringBuffer.append(
                        String.valueOf(job_.get("trade_id")) + "\t" +
                                String.valueOf(job_.get("quantity")) + "\t" +
                                String.valueOf(job_.get("mapping_id")) + "\t" +
                                String.valueOf(job_.get("trade_id")) + "\t" +
                                String.valueOf(job_.get("item_id")) + "\t" +
                                String.valueOf(job_.get("item_url")) + "\t" +
                                String.valueOf(job_.get("item_pic")) + "\t" +
                                String.valueOf(job_.get("item_name")) + "\t" +
                                String.valueOf(job_.get("original")) + "\t" +
                                String.valueOf(job_.get("real_total")) + "\r\n");
            }
        }
        String s = stringBuffer.toString();
        ReadWriteUtil.write("E:\\ls\\1215.txt", s);
    }

    public static void readFileByLines(String fileName) {
        File file = null;
        try {
//            SshUtil.getContext("192.168.15.196","root","yinghuo#123", fileName);
//            file = new File("d:/tmp/b.txt");
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
                        insertIntoHbase_userinfo(userinfoJson);
                        //  淘宝最近的送货地址
                        JSONArray recentdeliveraddressJarr = jobj.getJSONArray("recentdeliveraddress");  //  数组
                        insertIntoHbase_recentdeliveraddress(recentdeliveraddressJarr);
                        //  淘宝送货地址
                        JSONArray deliveraddressJarr = jobj.getJSONArray("deliveraddress");  //  数组
                        insertIntoHbase_deliveraddress(deliveraddressJarr);
                        //  淘宝交易细节店铺信息
                        JSONObject tradedetailsJson = (JSONObject) jobj.get("tradedetails");
                        JSONArray tradedetailsJarr = tradedetailsJson.getJSONArray("tradedetails");
                        insertIntoHbase_tradedetails(tradedetailsJarr);
                        //  淘宝交易细节商品信息
                        insertIntoHbase_suborders(tradedetailsJarr);
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

    public static void main(String[] args) {
        readFileByLines("E:\\ls\\1215\\a28e8e9a-b985-11e7-aa28-00163e1385a8.json");
    }


}
