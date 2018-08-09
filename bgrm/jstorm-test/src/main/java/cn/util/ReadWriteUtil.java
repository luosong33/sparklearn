package cn.util;

import java.io.*;

public class ReadWriteUtil {

    //  读取文件
    public static String reads(String inpath, String fileEncoding) {
        StringBuffer sb = new StringBuffer();
        try {
            FileInputStream inputStream = new FileInputStream(inpath);
            //  字节流中指定编码
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, fileEncoding));
            String s = null;
            while ((s = bufferedReader.readLine()) != null) {
                //  逻辑
                sb.append(s + "\r\n");
            }
            inputStream.close();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString().split("\r\n")[0];
    }

    //  写单个文件（追加）
    public static void write(String path,String content){
        try {
            /* 写入文件 */
            File file = new File(path);
            if (!file.exists()) {  // 如果文件不存在则创建
                file.createNewFile();
            }
//            BufferedWriter out = new BufferedWriter(new FileWriter(writename, true));  //  追加写文件
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));  //  追加  加true
            out.write(content+"\r\n"); // \r\n即为换行
            out.flush(); // 把缓存区内容压入文件
            out.close(); // 最后记得关闭文件
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //  写单个文件（追加）
    public static void write(String path,String content, String encoding){
        try {
            /* 写入文件 */
            File file = new File(path);
            if (!file.exists()) {  // 如果文件不存在则创建
                file.createNewFile();
            }
//            BufferedWriter out = new BufferedWriter(new FileWriter(writename, true));  //  追加写文件
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), encoding));  //  追加  加true
            out.write(content+"\r\n"); // \r\n即为换行
            out.flush(); // 把缓存区内容压入文件
            out.close(); // 最后记得关闭文件
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
