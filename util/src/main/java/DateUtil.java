import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DateUtil {

    /**
     * 获取两个日期之间的所有日期（yyyy-MM-dd）
     *
     * @date 2017-4-1
     */
    public static List<String> getBetweenDates(String begin_, String end_) {
        List<String> result = new ArrayList<String>();
        Calendar tempStart = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date begin = null;
        Date end = null;
        try {
            begin = sdf.parse(begin_);
            end = sdf.parse(end_);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        tempStart.setTime(begin);
        while (begin.getTime() <= end.getTime()) {
            String s = sdf.format(tempStart.getTime());
            result.add(s);
            tempStart.add(Calendar.DAY_OF_YEAR, 1);
            begin = tempStart.getTime();
        }
        return result;
    }

    /**
     * 获取两个日期之间的所有日期（yyyy-MM-dd）,包含头和尾
     */
    public static List<String> getBetweenDates(String begin_, String end_, String intdf, String outdf) {
        List<String> result = new ArrayList<>();
        Calendar tempStart = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(intdf);
        SimpleDateFormat sdf_ = new SimpleDateFormat(outdf);
        Date begin = null;
        Date end = null;
        try {
            begin = sdf.parse(begin_);
            end = sdf.parse(end_);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        tempStart.setTime(begin);
        while (begin.getTime() <= end.getTime()) {
            String s = sdf_.format(tempStart.getTime());
            result.add(s);
            tempStart.add(Calendar.DAY_OF_YEAR, 1);
            begin = tempStart.getTime();
        }
        return result;
    }

    //  返回一个日期前多少天的日期
    public static String getFrontAfterDate(String date, int i) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date begin = null;
        try {
            begin = sdf.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(begin);
        calendar.add(Calendar.DATE, i);
        Date resDate = calendar.getTime();
        return sdf.format(resDate);
    }

    //  按固定返回一个日期前（或后, 负前正后）多少天的日期
    public static String getFrontAfterDate(String date, int i, String dfstr) {
        SimpleDateFormat sdf = new SimpleDateFormat(dfstr);
        Date begin = null;
        try {
            begin = sdf.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(begin);
        calendar.add(Calendar.DATE, i);
        Date resDate = calendar.getTime();
        return sdf.format(resDate);
    }

    //  按固定返回一个日期前（或后, 负前正后）多少天的日期
    public static String getFrontAfterDate(String date, int i, String indfstr, String outdfs) {
        SimpleDateFormat indf = new SimpleDateFormat(indfstr);
        Date begin = null;
        try {
            begin = indf.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(begin);
        calendar.add(Calendar.DATE, i);
        Date resDate = calendar.getTime();
        SimpleDateFormat outdf = new SimpleDateFormat(outdfs);
        return outdf.format(resDate);
    }

    /**
     * 获取month个月前的时间点（yyyy-MM-dd）
     * end_  时间     month   几个月
     */
    public static String getMonth(String end_, int month) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar tempStart = Calendar.getInstance();
        try {
            tempStart.setTime(format.parse(end_));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        tempStart.add(Calendar.MONTH, month);
        Date end = tempStart.getTime();
        String mon = format.format(end);
        return mon;
    }

    /**
     * 获取month个月前的时间点（yyyy-MM-dd）
     * end_  时间     month   几个月
     */
    public static String getMonth(String date, int month, String indfs, String outdfs) {
        SimpleDateFormat indf = new SimpleDateFormat(indfs);
        Calendar tempStart = Calendar.getInstance();
        try {
            tempStart.setTime(indf.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        tempStart.add(Calendar.MONTH, month);
        Date end = tempStart.getTime();
        SimpleDateFormat outdf = new SimpleDateFormat(outdfs);
        String mon = outdf.format(end);
        return mon;
    }

    //  获取当前时间
    public static String nowString() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,sss");//设置日期格式
        return df.format(new Date());
    }

    //  按格式获取当前时间
    public static String nowString(String dfstr) {
        SimpleDateFormat df = new SimpleDateFormat(dfstr);//设置日期格式   yyyy-MM-dd
        return df.format(new Date());
    }

    //  按格式返回指定日期字符串
    public static String getDateString(String str, String indfs, String outdfs) {
        SimpleDateFormat idf = new SimpleDateFormat(indfs);  //  设置日期格式
        Date date = null;
        try {
            date = idf.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        SimpleDateFormat odf = new SimpleDateFormat(outdfs);  //  设置日期格式
        return odf.format(date);
    }

    //  返回两个时间差
    public static String getDateDiff(String startDate, String endDate, String indfs, String outdfs) throws ParseException {
        SimpleDateFormat indf = new SimpleDateFormat(indfs);  //  设置日期格式
        SimpleDateFormat outdf = new SimpleDateFormat(outdfs);  //  设置日期格式
        long s = 0;
        long m = 0;
        long h = 0;
        long d = 0;
//        try {
        Date startdate = indf.parse(startDate);
        Date enddate = outdf.parse(endDate);
        long time1 = startdate.getTime();
        long time2 = enddate.getTime();
        long diff;
        if (time1 < time2) {
            diff = time2 - time1;
        } else {
            diff = time1 - time2;
        }
        s = (diff / 1000);              //  秒
        m = (diff / 1000 / 60);           //  分
        h = (diff / 1000 / 60 / 60);        //  时
        d = (diff / 1000 / 60 / 60 / 24);     //  天
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
        return String.valueOf(s + "|" + m + "|" + h + "|" + d);
    }

    public static String yearsBetween(String start, String end, String dfs) throws ParseException {
        Calendar startDate = Calendar.getInstance();
        Calendar endDate = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(dfs);
        startDate.setTime(sdf.parse(start));
        endDate.setTime(sdf.parse(end));

        Date startdate = sdf.parse(start);
        Date enddate = sdf.parse(end);
        long time1 = startdate.getTime();
        long time2 = enddate.getTime();
        int i;
        if (time1 < time2) {
            i = endDate.get(Calendar.YEAR) - startDate.get(Calendar.YEAR);
        } else {
            i = startDate.get(Calendar.YEAR) - endDate.get(Calendar.YEAR);
        }
        return i+"";
    }

    /*public static final String YYYY = "yyyy";
    public static final String YYYYMM = "yyyy-MM";
    public static final String YYYYMMDD = "yyyy-MM-dd";
    public static final String YYYYMMDDHH= "yyyy-MM-dd HH";
    public static final String YYYYMMDDHHMM = "yyyy-MM-dd HH:mm";
    public static final String YYYYMMDDHHMMSS = "yyyy-MM-dd HH:mm:ss";*/
    public static long getBetween(String beginTime, String endTime, String formatPattern, int returnPattern) throws ParseException{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatPattern);
        Date beginDate = simpleDateFormat.parse(beginTime);
        Date endDate = simpleDateFormat.parse(endTime);

        Calendar beginCalendar = Calendar.getInstance();
        Calendar endCalendar = Calendar.getInstance();
        beginCalendar.setTime(beginDate);
        endCalendar.setTime(endDate);
        switch (returnPattern) {
            case 0:
                return getByField(beginCalendar, endCalendar, Calendar.YEAR);
            case 1:
                return getByField(beginCalendar, endCalendar, Calendar.YEAR)*12 + getByField(beginCalendar, endCalendar, Calendar.MONTH);
            case 2:
                return getTime(beginDate, endDate)/(24*60*60*1000);
            case 3:
                return getTime(beginDate, endDate)/(60*60*1000);
            case 4:
                return getTime(beginDate, endDate)/(60*1000);
            case 5:
                return getTime(beginDate, endDate)/1000;
            default:
                return 0;
        }
    }
    private static long getByField(Calendar beginCalendar, Calendar endCalendar, int calendarField){
        return endCalendar.get(calendarField) - beginCalendar.get(calendarField);
    }
    private static long getTime(Date beginDate, Date endDate){
        return endDate.getTime() - beginDate.getTime();
    }

    public static void main(String[] args) throws Exception {
//        System.out.println(getMonth("2017-08-03 10:53:17", -3));
//        System.out.println(getFrontAfterDate("2020年08月28日",-1096,"yyyy年MM月dd日", "yyyy-MM-dd"));
//        System.out.println(getFrontAfterDate("2017-03-22", 363, "yyyy-MM-dd", "yyyy年MM月dd日"));
        List<String> dates = getBetweenDates("2018-03-20 00:00:00", "2018-03-30 00:00:00", "yyyy-MM-dd HH:mm:ss", "yyyyMMdd");
//        List<String> dates = getBetweenDates("20001017", "20171107", "yyyyMMdd");
//        System.out.println(getFrontAfterDate("2017-09-20", -40));
//        System.out.println(getFrontAfterDate(nowString("yyyyMMdd"), -1, "yyyyMMdd"));
//        System.out.println(getDateString("1115","MM-dd"));
//        System.out.println(getDateDiff("2017-08-15 12:44:27","2017-08-15 12:34:27", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss"));
//        System.out.println(yearsBetween("2012-12-15 12:12:12", "2013-01-14 11:12:12", "yyyy-MM-dd HH:mm:ss"));
//        System.out.println(getBetween("2012-12-31 12:12:12", "2013-01-01 11:12:12", "yyyy-MM-dd HH:mm:ss", 0));
        System.out.println();
    }
}
