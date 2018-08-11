package yyyq.util;


public class RowKeyUtil {

    //  分布式唯一id
    public static String getIdWorker(int i){
        IdWorker worker = new IdWorker(i);
        Long nextId = worker.nextId();
        return nextId.toString();
    }

    /**
     * 1手机号  2时间戳
     * result  反转手机号_分布式唯一id
     */
    public static String rowKeyUtil(String arg){
        IdWorker worker = new IdWorker(5);
        Long nextId = worker.nextId();
        String str = reverseStr(arg);  //  反转手机号
        String str1 = str+"_"+nextId;
        return str1;
    }

    public static String reverseStr(String arg){

        String str = "";
        try {
            str = new StringBuilder(arg).reverse().toString();
        } catch (Exception e) {
//            e.printStackTrace();
        }
        return str;
    }

    //  手机号反转，防止存储热点
//  def reverse(args: String): String = if (args.length == 0) args else reverse(args.substring(1, args.length)) + args.substring(0, 1)

  public static void main(String[] args){
      System.out.println(rowKeyUtil("18715818298"));
  }

}
