package yyyq.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PhoneUtils {
    public static String[] phonePrefix = new String[]{"133","153","180","181","189","173","177","130","131","132","155","156","185","186","145","176","185","134","135","136","137","138","139","150","151","152","158","159","182","183","184","147","178","184"};
    public static Set<String> phonePrefixSet = new HashSet<String>(Arrays.asList(phonePrefix));
    public static boolean isPhone(String phone){
        if(phone.length()==11&&phonePrefixSet.contains(phone.substring(0,3))){
            return true;
        }
        return false;
    }
    public static String setToString(HashSet<String> phoneSet){
        StringBuffer sb=new StringBuffer();
        for(String phone:phoneSet){
            sb.append("'"+phone+"',");
        }
        if(sb.toString().length()>0){
            return sb.toString().substring(0, sb.toString().length()-1);
        }
        return "";
    }
}
