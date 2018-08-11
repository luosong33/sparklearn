package yyyq.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;

/**
 * Created by Administrator on 2017/7/4.
 */
public class CalculationUtil {

    /**
     * 提供精确加法计算的add方法
     *
     * @param value1 被加数
     * @param value2 加数
     * @return 两个参数的和
     */
    public static String add(String value1, String value2) {
        BigDecimal b1 = new BigDecimal(value1);
        BigDecimal b2 = new BigDecimal(value2);
        BigDecimal add = b1.add(b2);
        return String.valueOf(add);
    }

    /**
     * 提供精确减法运算的sub方法
     *
     * @param value1 被减数
     * @param value2 减数
     * @return 两个参数的差
     */
    public static String sub(String value1, String value2) {
        BigDecimal b1 = new BigDecimal(value1);
        BigDecimal b2 = new BigDecimal(value2);
        BigDecimal sub = b1.subtract(b2);
        return String.valueOf(sub);
    }

    /**
     * 提供精确乘法运算的mul方法
     *
     * @param value1 被乘数
     * @param value2 乘数
     * @return 两个参数的积
     */
    public static String mul(String value1, String value2) {
        BigDecimal b1 = new BigDecimal(value1);
        BigDecimal b2 = new BigDecimal(value2);
        BigDecimal multiply = b1.multiply(b2);
        return String.valueOf(multiply);
    }

    /**
     * 提供精确乘法运算的mul方法，并保留n位小数
     *
     * @param value1 被乘数
     * @param value2 乘数
     * @return 两个参数的积
     */
    public static String mul(String value1, String value2, int dec) {
        BigDecimal b1 = new BigDecimal(value1);
        BigDecimal b2 = new BigDecimal(value2);
        BigDecimal multiply = b1.multiply(b2);
        String s = String.valueOf(multiply);
        String[] split = s.split("\\.");
        s = split[0] + "." + split[1].substring(0, dec);
        return s;
    }

    /**
     * 提供精确的除法运算方法div，无scale
     *
     * @param value1 被除数
     * @param value2 除数
     * @return 两个参数的商
     * @throws IllegalAccessException
     */
    public static String div(String value1, String value2) {
        BigDecimal b1 = new BigDecimal(value1);
        BigDecimal b2 = new BigDecimal(value2);
        BigDecimal v = new BigDecimal(0);
        try {
            v = b1.divide(b2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return String.valueOf(v);
    }

    /**
     * 提供精确的除法运算方法div，截取商，截取保留几位小数，不舍入
     * @param value1 被除数
     * @param value2 除数
     * @return 两个参数的商
     * @throws IllegalAccessException
     */
    public static String div(String value1, String value2, int scale) {
        if (scale < 0) {
            try {
                throw new IllegalAccessException("精确度不能小于0");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        BigDecimal b1 = new BigDecimal(value1);
        BigDecimal b2 = new BigDecimal(value2);
        BigDecimal v = new BigDecimal(0);
        try {
//            v = b1.divide(b2);
            v = b1.divide(b2, scale + 1, BigDecimal.ROUND_HALF_EVEN);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String s = String.valueOf(v);
        /*String[] split = s.split("\\.");
        s = split[0] + "." + split[1].substring(0, dec);*/
        return bigDecimal(s, scale);
    }

    /**
     * 提供精确的除法运算方法div, 舍入商，scale 精确范围
     * @param value1 被除数
     * @param value2 除数
     * @param scale  精确范围
     * @return 两个参数的商
     * @throws IllegalAccessException
     */
    public static String div_scale(String value1, String value2, int scale) {
        //如果精确范围小于0，抛出异常信息
        if (scale < 0) {
            try {
                throw new IllegalAccessException("精确度不能小于0");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        BigDecimal b1 = new BigDecimal(value1);
        BigDecimal b2 = new BigDecimal(value2);
        BigDecimal divide = b1.divide(b2, scale, BigDecimal.ROUND_HALF_EVEN);
        return String.valueOf(divide);
    }

    //  只保留几位小数，不舍入
    public static String bigDecimal(String dvalue, int i) {
        BigDecimal bigDecimal = new BigDecimal(dvalue);
        bigDecimal.setScale(2, BigDecimal.ROUND_DOWN);

        StringBuffer df = new StringBuffer("0.");
        for (int j = 0; j < i; j++) {
            df.append("0");
        }
        String s = df.toString();
        DecimalFormat decimalFormat = new DecimalFormat(s);  //  保留两位小数  例0.00  等价于  "#,###.00"
        decimalFormat.setRoundingMode(RoundingMode.DOWN);
        return decimalFormat.format(bigDecimal);
    }


    public static void main(String[] args) throws IllegalAccessException {
        System.out.println(0.06 + 0.01);
        System.out.println(1.0 - 0.42);
        System.out.println(4.015 * 100);
        System.out.println(303.1 / 1000);
        System.out.println("-----------------------------------------");
        System.out.println(add("0.06", "0.01"));
        System.out.println(sub("1.0", "0.42"));
        System.out.println("---------------------mul--------------------");
        System.out.println(mul("4.015333", "100"));
        System.out.println(mul("4.015333", "100", 3));
        System.out.println("---------------------div--------------------");
        System.out.println(div("11111", "10", 2));
        System.out.println(div_scale("11111", "10", 2));
        System.out.println(div("11111", "33", 2));  //  336.6969696969697
        System.out.println(div_scale("11111", "33", 2));

    }


}
