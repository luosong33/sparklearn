package yyyq.rm;

import net.sf.json.JSONObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;

public class HTTPUtils {

	public static void httpPostWithJson(JSONObject jsonObj, String url) {
		
		HttpPost post = null;
		try {
			HttpClient httpClient=HTTPUtils.getHttpClient();
			post = new HttpPost(url);
			// 构造消息头
			post.setHeader("Content-type", "application/json; charset=utf-8");
			post.setHeader("Connection", "Close");
			// 构建消息实体
			StringEntity entity = new StringEntity(jsonObj.toString());
			entity.setContentEncoding("UTF-8");
			// 发送Json格式的数据请求
			entity.setContentType("application/json");
			post.setEntity(entity);
			HttpResponse response = httpClient.execute(post);
		        // 检验返回码
		    int statusCode = response.getStatusLine().getStatusCode();
		    System.out.println(statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (post != null) {
				try {
					post.releaseConnection();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void httpPostWithJson(JSONObject jsonObj, String url, HttpClient httpClient) {

		HttpPost post = null;
		try {
			post = new HttpPost(url);
			// 构造消息头
			post.setHeader("Content-type", "application/json; charset=utf-8");
			post.setHeader("Connection", "Close");
			// 构建消息实体
			StringEntity entity = new StringEntity(jsonObj.toString());
			entity.setContentEncoding("UTF-8");
			// 发送Json格式的数据请求
			entity.setContentType("application/json");
			post.setEntity(entity);
			HttpResponse response = httpClient.execute(post);
		        // 检验返回码
		    int statusCode = response.getStatusLine().getStatusCode();
		    System.out.println(statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (post != null) {
				try {
					post.releaseConnection();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}


	private static HttpClient httpClient = null;
	public static HttpClient getHttpClient() {
		if (httpClient == null) {
			httpClient = new DefaultHttpClient();
			httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 1000);
			httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 2000);
		}
		return httpClient;
	}
}
