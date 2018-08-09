package yyyq.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


public class MoxieInsertToHBase extends Configured {
	static class WholeFileInputFormat extends FileInputFormat{  
        @Override  
        protected boolean isSplitable(JobContext context, Path filename) {  
            return false;  
        }  
        @Override  
        public RecordReader createRecordReader(InputSplit split,  
                TaskAttemptContext context) throws IOException,  
                InterruptedException {  
              
            return new WholeFileRecordReader();  
        }  
    }  
    static  class WholeFileRecordReader extends RecordReader{  
        private FileSplit fileSplit;  
        private FSDataInputStream fis;  
        private Text key = null;  
        private Text value = null;  
          
        private boolean processed = false;  
        @Override  
        public void initialize(InputSplit inputSplit, TaskAttemptContext context)  
                throws IOException, InterruptedException {  
            fileSplit = (FileSplit)inputSplit;  
            Configuration job = context.getConfiguration();  
            Path file = fileSplit.getPath();  
            FileSystem fs = file.getFileSystem(job);  
            fis = fs.open(file);  
        }  
  
        @Override  
        public boolean nextKeyValue() throws IOException, InterruptedException {  
              
            if(key == null){  
                key = new Text();  
            }  
            if(value == null){  
                value = new Text();  
            }  
            if(!processed){  
                byte[] content = new byte[(int)fileSplit.getLength()];  
                Path file = fileSplit.getPath();  
                key.set(file.toString());  
                  
                org.apache.hadoop.io.IOUtils.readFully(fis, content, 0, content.length);  
                String sendString=new String(  content , "GBK" );  
                value.set(new Text(sendString));  
            processed = true;  
            return true;  
            }  
            return false;  
        }  
  
        @Override  
        public Text getCurrentKey() throws IOException, InterruptedException {  
            // TODO Auto-generated method stub  
            return this.key;  
        }  
  
        @Override  
        public Text getCurrentValue() throws IOException,  
                InterruptedException {  
              
            return this.value;  
        }  
  
        @Override  
        public float getProgress() throws IOException, InterruptedException {  
            // TODO Auto-generated method stub  
            return processed ? fileSplit.getLength() : 0;  
        }  
  
        @Override  
        public void close() throws IOException {  
              
              
        }  
          
    }  
	public static class InsertBaseMapper extends Mapper<Text, Text, Text, Text> {
		private Text mapKey = new Text();
		private Text mapValue=new Text();
        public void map(Text key,Text value,Context context) throws IOException, InterruptedException{
        	String tempKey=key.toString().substring(0,key.toString().lastIndexOf("/"));
    		String token=tempKey.substring(tempKey.lastIndexOf("/")+1);
    		String tempVoucherNo=tempKey.substring(0,tempKey.lastIndexOf("/"));
    		String voucherNo=tempVoucherNo.substring(tempVoucherNo.lastIndexOf("/")+1);
    		JSONObject json = JSON.parseObject(value.toString());
			String mobile=json.getString("mobile");
			String calls = json.getString("calls");
			JSONArray pushDataJson = JSONArray.parseArray(calls);
			for (Iterator<Object> iterator = pushDataJson.iterator(); iterator.hasNext();) {
				JSONObject job = (JSONObject) iterator.next();
				String calllist = job.getString("items");
				JSONArray jarr = JSONArray.parseArray(calllist);
				for (Iterator<Object> iterator2 = jarr.iterator(); iterator2.hasNext();) {
					JSONObject jobb = (JSONObject) iterator2.next();
					String time = jobb.getString("time");
					String location = jobb.getString("location");
					String fee = jobb.getString("fee");
					String details_id = jobb.getString("details_id");
					String peer_number = jobb.getString("peer_number");
					String location_type = jobb.getString("location_type");
					String duration = jobb.getString("duration");
					String dial_type = jobb.getString("dial_type");
					mapKey.set(voucherNo+"_"+token);
					mapValue.set(time+"_"+location+"_"+fee+"_"+details_id+"_"+peer_number+"_"+location_type+"_"+duration+"_"+dial_type+"_"+mobile+"_"+token+"_"+voucherNo);
					context.write(mapKey,mapValue);  
				}
			}
        }

    }
	public static class InsertBaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable>{
        @SuppressWarnings("deprecation")
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
        	String voucherNo=key.toString().split("_")[0];
        	String token=key.toString().split("_")[1];
            for(Text val:values){
            	String time = val.toString().split("_")[0];
				String location = val.toString().split("_")[1];
				String fee = val.toString().split("_")[2];
				String details_id = val.toString().split("_")[3];
				String peer_number = val.toString().split("_")[4];
				String location_type = val.toString().split("_")[5];
				String duration = val.toString().split("_")[6];
				String dial_type = val.toString().split("_")[7];
				String mobile = val.toString().split("_")[8];
				String rowKey=voucherNo + "_" + mobile + "_"+ peer_number + "_" + time;
				Put put = new Put(rowKey.getBytes());
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("time"),Bytes.toBytes(time));
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("location"),Bytes.toBytes(location));
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("fee"),Bytes.toBytes(fee));
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("details_id"),Bytes.toBytes(details_id));
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("peer_number"),Bytes.toBytes(peer_number));
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("location_type"),Bytes.toBytes(location_type));
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("duration"),Bytes.toBytes(duration));
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("dial_type"),Bytes.toBytes(dial_type));
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("mobile"),Bytes.toBytes(mobile));
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("token"),Bytes.toBytes(token));
	            put.add(Bytes.toBytes("c"),Bytes.toBytes("voucherNo"),Bytes.toBytes(voucherNo));
	            context.write(null, put);
            }
        }
    }

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ClassNotFoundException, InterruptedException {
        // TODO Auto-generated method stub
        String tableName = "moxie_basic_call_details";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.15.195");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //System.setProperty("HADOOP_USER_NAME", "root");
       /*HBaseAdmin admin = new HBaseAdmin(conf);
        //如果表格存在就删除
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor =new HColumnDescriptor("c");
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);*/
        Job job = new Job(conf,"upload to hbase");
        job.setJarByClass(MoxieInsertToHBase.class);
        job.setMapperClass(InsertBaseMapper.class);
        TableMapReduceUtil.initTableReducerJob(tableName, InsertBaseReducer.class, job);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
//        String commaSeparatedPaths=FileSystemAPI.ListFile("/tmp/nfs/cdsp/moxie_carrier/basic/20170927/");
//        FileInputFormat.setInputPaths(job, commaSeparatedPaths);/*.addInputPaths(job, "hdfs://192.168.15.195:8020/tmp/nfs/cdsp/");*/
        System.exit(job.waitForCompletion(true)?0:1);

    }
}
