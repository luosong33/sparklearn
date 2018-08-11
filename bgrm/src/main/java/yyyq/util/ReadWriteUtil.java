package yyyq.util;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReadWriteUtil {

    //  读取文件    返回整个文件字符串
    public static String reads(String inpath, String fileEncoding) {
        StringBuffer sb = new StringBuffer();
        try {
            FileInputStream inputStream = new FileInputStream(inpath);
            //  字节流中指定编码
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, fileEncoding));
            String s = null;
            while ((s = bufferedReader.readLine()) != null) {
                sb.append(s + "\r\n");
            }
            inputStream.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String res = sb.toString();
        Pattern p = Pattern.compile("$\r\n");  //  去掉最后一个换行符
        Matcher m = p.matcher(res);
        res = m.replaceAll("");
        return res;
    }

    //  写单个文件（追加式）
    public static synchronized void write(String path, String content) {
        try {
            /* 写入文件 */
            File file = new File(path);
            if (!file.exists()) {  // 如果文件不存在则创建
                file.createNewFile();
            }
//            BufferedWriter out = new BufferedWriter(new FileWriter(writename, true));  //  追加写文件
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));  //  追加  加true
            out.write(content + "\r\n"); // \r\n即为换行
            out.flush(); // 把缓存区内容压入文件
            out.close(); // 最后记得关闭文件
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //  写单个文件（清空式）
    public static void closeWrite(String path, String content) {
        try {
            /* 写入文件 */
            File file = new File(path);
            if (!file.exists()) {  // 如果文件不存在则创建
                file.createNewFile();
            }
            FileWriter fileWriter =new FileWriter(file);
            fileWriter.write("");
            fileWriter.flush();
            fileWriter.close();

            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));  //  追加  加true
            out.write(content + "\r\n"); // \r\n即为换行
            out.flush(); // 把缓存区内容压入文件
            out.close(); // 最后记得关闭文件
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
