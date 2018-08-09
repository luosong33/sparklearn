package yyyq.kafka.cdspthread;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import yyyq.util.DateUtil;
import yyyq.util.ReadWriteUtil;

public class Cdsp2HandlerProducer implements Runnable {

    private String message;
    public Cdsp2HandlerProducer(String message) {
        this.message = message;  
    }  
  
    @Override  
    public void run() {
        String reads = null;
        String clientNo = null;
        String creditNo = null;
        String origDate = null;
        String apiCode = null;
        try {
            origDate = message.split(" - \\(")[0];
            clientNo = message.split("clientNo:")[1].split(",")[0];
            creditNo = message.split("creditNo:")[1].split(",")[0];
            apiCode = message.split("apiCode:")[1].split(",")[0];
            String filePath = message.split("filePath:")[1].split("\\)")[0];
            reads = ReadWriteUtil.reads(filePath, "GBK");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("数据解析错误");
        }
        JSONObject jsonObject = JSON.parseObject(reads);
        jsonObject.put("clientNo", clientNo);
        jsonObject.put("creditNo", creditNo);
        jsonObject.put("origDate", origDate);
        String str = JSON.toJSONString(jsonObject);
        Cdsp2KafkaProducerSingleton kafkaProducerSingleton = Cdsp2KafkaProducerSingleton.getInstance();
        /*if ("MOXIE_CARRIER_ACCESS_SUMMIT".equals(apiCode)){
            apiCode = "cdsp_thread_test";
        }else {
            apiCode = "test_thread";
        }*/
        kafkaProducerSingleton.init(apiCode+"_HISTORY");
        kafkaProducerSingleton.sendKafkaMessage(str);
        System.out.println(message.split(",filePath:")[0] + ")"+" | "+DateUtil.nowString());
    }
  
}