package yyyq.util.druidOrPool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Queue;
import java.util.LinkedList;

/**
 简易生成并集中管理一个连接池
 */
public class CurrencyConnPool {
    private static LinkedList<Connection> queue = null;

    private CurrencyConnPool() {
//        CurrencyConnPool.getInser();
        /*if (queue == null){
            queue = Inner.queue;
        }*/
    }

    public static void getInser(){
        if (queue == null){
            queue = Inner.queue;
        }
    }

    //  初始化池，静态内部类，既实现了线程安全，又避免了同步带来的性能影响
    private static class Inner {
        private static LinkedList<Connection> queue = new LinkedList<>();

        static {
            try {
                for (int i = 0; i < 50; i++) {
                    Connection conn = null;
                    conn = DriverManager.getConnection(
                            "jdbc:mysql://192.168.15.198:3306/test",
                            "root",
                            "rootROOT1."
                    );
                    queue.push(conn);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static synchronized Connection getConnection() {
        Connection con = null;
        if (queue.size() > 0) {
            con = queue.poll();
        } else {
            con = getConnection();  //  当取出的连接为null时,递归调用自己,直到获得一个可用连接为止
        }
        return con;
    }

    public static void returnConn(Connection conn) {
        queue.offer(conn);
    }


    public static void main(String[] args) {
        CurrencyConnPool.getInser();
        Connection connection0 = CurrencyConnPool.getConnection();
        Connection connection1 = CurrencyConnPool.getConnection();
        Connection connection2 = CurrencyConnPool.getConnection();

        CurrencyConnPool.returnConn(connection1);

        Queue<String> queue = new LinkedList<>();
        queue.offer("Hello");
        queue.offer("World!");
        queue.offer("你好！");
        System.out.println(queue.size());
        String str;
        while ((str = queue.poll()) != null) {
            System.out.print(str);
        }
        System.out.println();
        System.out.println(queue.size());
    }

}
