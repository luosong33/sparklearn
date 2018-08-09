package yyyq.datasync.jingshuang;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* 导出表成文件 */
public class YyydInfoHbase2File {

    static class HbaseMapper extends TableMapper<ImmutableBytesWritable, Text>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,Context context)
				throws IOException, InterruptedException {
			for(Cell cell :value.rawCells()){
				context.write(new ImmutableBytesWritable("c".getBytes()),
						new Text(Bytes.toString(CellUtil.cloneQualifier(cell))+","+Bytes.toString(CellUtil.cloneValue(cell))));
			}
		}
	}

	static class HbaseReduce extends Reducer<ImmutableBytesWritable, Text, Text, Text>{
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			for(Text text:values){
				context.write(new Text("c"), text);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "SH-M1-L06-YH-node1,SH-M1-L06-YH-node2,SH-M1-L06-YH-node3,SH-M1-L06-YH-node4,SH-M1-L06-YH-node5");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("df.default.name", "hdfs://master:8020/");//设置hdfs的默认路径
		Job job = Job.getInstance(conf,"YyydInfoHbase2File");
		job.setJarByClass(YyydInfoHbase2File.class);
		
		List<Scan> list = new ArrayList<>();
		Scan scan = new Scan();
		scan.setCaching(200);
		scan.setCacheBlocks(false);
		/*scan.setStartRow("row1".getBytes());
		scan.setStopRow("row5".getBytes());*/
		scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("clientNo"));
		scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("linker"));
		scan.addColumn(Bytes.toBytes("c"), Bytes.toBytes("linkPhone"));
		scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "yyyd_info_result".getBytes());
		list.add(scan);
		
		TableMapReduceUtil.initTableMapperJob(list, HbaseMapper.class, ImmutableBytesWritable.class,Text.class, job);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(HbaseReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path("/bigdata/production/jingshuang"));
			
		System.exit(job.waitForCompletion(true)==true?0:1);
		
		
		
	}
	
	
}