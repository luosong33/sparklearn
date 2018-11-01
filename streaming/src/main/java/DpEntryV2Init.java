import com.alibaba.fastjson.JSONObject;
import com.anve.datacenter.api.dp.DpEntryV2;

/**
 * Created by genie on 2018/10/28.
 */
public class DpEntryV2Init {

    public static DpEntryV2 InitDpEntryV2(String str) {
        JSONObject job = JSONObject.parseObject(str);
        DpEntryV2 dpEntryV2 = new DpEntryV2();
        Long userId = Long.valueOf((String) job.get("userId"));
        String event = (String) job.get("event");
        Long time = Long.valueOf((String)job.get("time"));
        String ip = (String) job.get("ip");
        String deviceId = (String) job.get("deviceId");
        String appVersion = (String) job.get("appVersion");
        String manufacturer = (String) job.get("manufacturer");
        String model = (String) job.get("model");
        String os = (String) job.get("os");
        String osVersion = (String) job.get("osVersion");
        Integer screenHeight = Integer.parseInt((String)job.get("screenHeight"));
        Integer screenWidth = Integer.parseInt((String)job.get("screenWidth"));
        Boolean wifi = Boolean.parseBoolean((String)job.get("wifi"));
        String carrier = (String) job.get("carrier");
        String networkType = (String) job.get("networkType");
        String properties = (String) job.get("properties");
        String day = (String) job.get("day");
        String month = (String) job.get("month");
        String year = (String) job.get("year");
        String traceId = (String) job.get("traceId");
        String screenName = (String) job.get("screenName");
        dpEntryV2.setUserId(userId);
        dpEntryV2.setEvent(event);
        dpEntryV2.setTime(time);
        dpEntryV2.setIp(ip);
        dpEntryV2.setDeviceId(deviceId);
        dpEntryV2.setAppVersion(appVersion);
        dpEntryV2.setManufacturer(manufacturer);
        dpEntryV2.setModel(model);
        dpEntryV2.setOs(os);
        dpEntryV2.setOsVersion(osVersion);
        dpEntryV2.setScreenHeight(screenHeight);
        dpEntryV2.setScreenWidth(screenWidth);
        dpEntryV2.setWifi(wifi);
        dpEntryV2.setCarrier(carrier);
        dpEntryV2.setNetworkType(networkType);
        dpEntryV2.setProperties(properties);
        dpEntryV2.setDay(day);
        dpEntryV2.setMonth(month);
        dpEntryV2.setYear(year);
        dpEntryV2.setTraceId(traceId);
        dpEntryV2.setScreenName(screenName);
        return dpEntryV2;
    }
}
