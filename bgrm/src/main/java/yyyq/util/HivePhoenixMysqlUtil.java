package yyyq.util;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HivePhoenixMysqlUtil {

    public static void main(String[] args) {
//        getPhoenixTable("mojie_report_tripinfo");
//        gethiveExTableAdd("rpt_xjb_data_0211");
//        gethiveInTableAdd("mojie_report_bank_creditcard_other_attribute_In");
//        getMysqlTableAdd("mojie_report_bank_creditcard_other_attribute");
//        getColumn("\"");
//        getMysqlTable("mojie_report_security_user_basic_info_check");
//        gethiveInTable("rpt_xjb_data_0211");
        gethiveExTable("hcb_whole");
    }

    //  自带id
    private static void getPhoenixTable(String arg) {
        String reads = ReadWriteUtil.reads("data/hivePhoenixTable.properties", "UTF-8");
        String[] split = reads.split("\r\n");
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE TABLE IF NOT EXISTS \"" + arg + "\" (\r\n");
        sb.append("\"ID\" VARCHAR, \r\n");
        for (int i = 0; i < split.length; i++) {
            if (i == split.length - 1) {
                sb.append("\"c\".\"" + split[i] + "\" VARCHAR\r\n");
            } else {
                sb.append("\"c\".\"" + split[i] + "\" VARCHAR,\r\n");
            }
        }
        sb.append("CONSTRAINT pk PRIMARY KEY (\"ID\")\r\n" +
                ");");
        ReadWriteUtil.closeWrite("data/table.txt", sb.toString());
        System.out.println(sb.toString());
    }

    //  hive外部表  自带id
    private static void gethiveExTable(String arg) {
        String reads = ReadWriteUtil.reads("data/hivePhoenixTable.properties", "UTF-8");
        String[] split = reads.split("\r\n");
        StringBuffer sb = new StringBuffer();
        sb.append("create external table " + arg + "(\r\n");
        sb.append("ID string,\r\n");
        for (int i = 0; i < split.length; i++) {
            if (i == split.length - 1) {
                sb.append(split[i] + " string\r\n");
            } else {
                sb.append(split[i] + " string,\r\n");
            }
        }
        sb.append(") stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\r\n" +
                "with serdeproperties(\"hbase.columns.mapping\"=\"\r\n" +
                ":key,\r\n");
        for (int i = 0; i < split.length; i++) {
            if (i == split.length - 1) {
                sb.append("c:" + split[i] + "\r\n");
            } else {
                sb.append("c:" + split[i] + ",\r\n");
            }
        }
        sb.append("\")tblproperties(\"hbase.table.name\" = \"" + arg + "\");");
        ReadWriteUtil.closeWrite("data/table.txt", sb.toString());
        System.out.println(sb.toString());
    }

    //  hive内部表  自带id
    private static void gethiveInTable(String arg) {
        String reads = ReadWriteUtil.reads("data/hivePhoenixTable.properties", "UTF-8");
        String[] split = reads.split("\r\n");
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE TABLE IF NOT EXISTS " + arg + " (\r\n");
        sb.append("ID string,\r\n");
        for (int i = 0; i < split.length; i++) {
            if (i == split.length - 1) {
                sb.append(split[i] + " string\r\n");
            } else {
                sb.append(split[i] + " string,\r\n");
            }
        }
        sb.append(")  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ;");
        ReadWriteUtil.closeWrite("data/table.txt", sb.toString());
        System.out.println(sb.toString());
    }

    //  mysql  自带id
    private static void getMysqlTable(String arg) {
        String reads = ReadWriteUtil.reads("data/hivePhoenixTable.properties", "UTF-8");
        String[] split = reads.split("\r\n");
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE TABLE `" + arg + "` (\r\n");
        sb.append("`id` varchar(64) NOT NULL,\r\n");
        for (String column : split) {
            sb.append("`" + column + "` varchar(256),\r\n");
        }
        sb.append("  PRIMARY KEY (`id`)\r\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
        ReadWriteUtil.closeWrite("data/table.txt", sb.toString());
        System.out.println(sb.toString());
    }

    //  hiva外部表  添加业务字段
    private static void gethiveExTableAdd(String arg) {
        String reads = ReadWriteUtil.reads("data/hivePhoenixTable.properties", "UTF-8");
        String[] split = reads.split("\r\n");
        StringBuffer sb = new StringBuffer();
        sb.append("create external table " + arg + "(\r\n");
        sb.append("ID string,\r\n");
        for (String column : split) {
            sb.append(column + " string,\r\n");
        }
        sb.append("client_no string,\r\n");
        sb.append("cert_no string,\r\n");
        sb.append("voucher_no string,\r\n");
        sb.append("loadtime string\r\n");
        sb.append(") stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\r\n" +
                "with serdeproperties(\"hbase.columns.mapping\"=\"\r\n" +
                ":key,\r\n");
        for (String column : split) {
            sb.append("c:" + column + ",\r\n");
        }
        sb.append("c:client_no,\r\n");
        sb.append("c:cert_no,\r\n");
        sb.append("c:voucher_no,\r\n");
        sb.append("c:loadtime\r\n");
        sb.append("\")tblproperties(\"hbase.table.name\" = \"" + arg + "\");");
        ReadWriteUtil.closeWrite("data/table.txt", sb.toString());
        System.out.println(sb.toString());
    }

    //  hive内部表  添加业务字段
    private static void gethiveInTableAdd(String arg) {
        String reads = ReadWriteUtil.reads("data/hivePhoenixTable.properties", "UTF-8");
        String[] split = reads.split("\r\n");
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE TABLE IF NOT EXISTS " + arg + " (\r\n");
        sb.append("ID string,\r\n");
        for (int i = 0; i < split.length; i++) {
            sb.append(split[i] + " string,\r\n");
        }
        sb.append("client_no string,\r\n");
        sb.append("cert_no string,\r\n");
        sb.append("voucher_no string,\r\n");
        sb.append("loadtime string\r\n");
        sb.append(")  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ;");
        ReadWriteUtil.closeWrite("data/table.txt", sb.toString());
        System.out.println(sb.toString());
    }

    //  mysql  自带id
    private static void getMysqlTableAdd(String arg) {
        String reads = ReadWriteUtil.reads("data/hivePhoenixTable.properties", "UTF-8");
        String[] split = reads.split("\r\n");
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE TABLE `" + arg + "` (\r\n");
        sb.append("`id` varchar(64) NOT NULL,\r\n");
        for (String column : split) {
            sb.append("`" + column + "` varchar(256),\r\n");
        }
        sb.append("`client_no` varchar(256),\r\n");
        sb.append("`cert_no` varchar(256),\r\n");
        sb.append("`voucher_no` varchar(256),\r\n");
        sb.append("`loadtime` varchar(256),\r\n");
        sb.append("  PRIMARY KEY (`id`)\r\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;");
        ReadWriteUtil.closeWrite("data/table.txt", sb.toString());
        System.out.println(sb.toString());
    }

    private static void getColumn(String sp) {
        String reads = ReadWriteUtil.reads("data/hivePhoenixTable.properties", "UTF-8");
        String[] split = reads.split("\r\n");
        StringBuffer sb = new StringBuffer();
        for (String column : split) {
            String s = column.split(sp)[0];
            sb.append(s + "\r\n");
        }
        String res = sb.toString();
        Pattern p = Pattern.compile("$\r\n");  //  去掉最后一个换行符
        Matcher m = p.matcher(res);
        res = m.replaceAll("");
        ReadWriteUtil.closeWrite("data/table.txt", res);
        System.out.println(sb.toString());
    }

}
