package yyyq.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {

    public static String getNumber(String str){
        String regEx="[^0-9]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        return m.replaceAll("").trim();
    }

    public static void main(String[] args) {
//        System.out.println(getOneMonth("2017-08-03 10:53:17", -3));
//        System.out.println(nowString());
        System.out.println(getNumber("1-33,2222, 4444     ,  134"));
    }

}
