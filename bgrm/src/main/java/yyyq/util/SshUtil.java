package yyyq.util;

import ch.ethz.ssh2.*;
import com.sshtools.j2ssh.SshClient;
import com.sshtools.j2ssh.authentication.AuthenticationProtocolState;
import com.sshtools.j2ssh.authentication.PasswordAuthenticationClient;
import com.sshtools.j2ssh.sftp.SftpFile;

import java.io.*;
import java.util.List;

public class SshUtil {

    public static SshClient getSshConn(String ip, String user, String pass) {
        SshClient client = new SshClient();
        try {
            client.connect(ip);
            //设置用户名和密码
            PasswordAuthenticationClient pwd = new PasswordAuthenticationClient();
            pwd.setUsername(user);
            pwd.setPassword(pass);
            int result = client.authenticate(pwd);
            if (result == AuthenticationProtocolState.COMPLETE) { // 如果连接完成
                return client;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return client;
    }

    public static void getContext(String ip, String user, String pass, String path) {
        Connection conn = null;
        Session session = null;
        try {
            conn = new Connection(ip);
            conn.connect();
            boolean isAuthenticated = conn.authenticateWithPassword(user, pass);
            if (!isAuthenticated) {
                throw new IOException("Authenticated failed");
            }

            //  执行命令
            /*session = conn.openSession();
            session.execCommand("df -h");
            InputStream in = new StreamGobbler(session.getStdout());
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            br.close();*/

            //  下载到本地
            SCPClient scpClient = conn.createSCPClient();
            SCPInputStream scpis = scpClient.get(path);
            File file = new File("d:/tmp/b.txt");
            file.delete();
            FileOutputStream fos = new FileOutputStream("d:/tmp/b.txt");
            byte[] buffer = new byte[1024];
            int len = 0;
            while ((len = scpis.read(buffer)) != -1) {
                fos.write(buffer, 0, len);
            }
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
//            session.close();
            conn.close();
        }
    }

    public static String getRemoteContext(String ip, String user, String pass) {
        SshClient client = new SshClient();

        try {
            client.connect(ip);
            //设置用户名和密码
            PasswordAuthenticationClient pwd = new PasswordAuthenticationClient();
            pwd.setUsername(user);
            pwd.setPassword(pass);
            int result = client.authenticate(pwd);
            if (result == AuthenticationProtocolState.COMPLETE) {//如果连接完成
                System.out.println("===============" + result);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void main(String[] args) {
        getContext("192.168.15.196","root","yinghuo#123", "/tmp/nfs/cdsp/hulu/report/20171031/8eaf33b25b724f46bae366cfe59c51c6.txt");
    }
}
