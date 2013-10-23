package in.naishe.mc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.StringTokenizer;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CassandraWordCount extends Configured implements Tool {

	public static class WordMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable>{
		private static final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		protected void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> cols, Context context)
		throws IOException, InterruptedException {
			
//			Iterate through the column values
			for(IColumn col: cols.values()){
				String val = ByteBufferUtil.string(col.value());
				StringTokenizer tokenizer = new StringTokenizer(val);
		        
		        while (tokenizer.hasMoreTokens()) {
		            word.set(tokenizer.nextToken());
		            context.write(word, ONE);
		        }
			}
        }
	}
	
	public static class WordReducer extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value: values){
				sum = sum + value.get();
			}
			Column col = new Column();
			col.setName(ByteBufferUtil.bytes("count"));
			col.setValue(ByteBufferUtil.bytes(sum));
			col.setTimestamp(System.currentTimeMillis());
			
			Mutation mutation = new Mutation();
			mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn());
			mutation.getColumn_or_supercolumn().setColumn(col);
			context.write(ByteBufferUtil.bytes(key.toString()), Collections.singletonList(mutation));
		}
	}
	
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "cassandrawordcount");
		job.setJarByClass(getClass());
		
//		Anything you set in conf will be available to Mapper and Reducer
		Configuration conf = job.getConfiguration();
		
//		set mapper and reducer
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);
		
//		Cassandra Specific settings for ingesting CF
		ConfigHelper.setInputInitialAddress(conf, Setup.CASSANDRA_HOST_ADDR);
		ConfigHelper.setInputRpcPort(conf, String.valueOf(Setup.CASSANDRA_RPC_PORT));
		ConfigHelper.setInputPartitioner(conf, RandomPartitioner.class.getName());
		ConfigHelper.setInputColumnFamily(conf, Setup.KEYSPACE, Setup.INPUT_CF);
		
		SliceRange sliceRange = new SliceRange(
										ByteBufferUtil.bytes(""), 
										ByteBufferUtil.bytes(""), 
										false,
										Integer.MAX_VALUE);
		SlicePredicate predicate = new SlicePredicate()
									.setSlice_range(sliceRange);
		ConfigHelper.setInputSlicePredicate(conf, predicate);
		
		job.setInputFormatClass(ColumnFamilyInputFormat.class);
		
		
//		Cassandra specific output setting
		ConfigHelper.setOutputInitialAddress(conf, Setup.CASSANDRA_HOST_ADDR);
		ConfigHelper.setOutputRpcPort(conf, String.valueOf(Setup.CASSANDRA_RPC_PORT));
		ConfigHelper.setOutputPartitioner(conf, RandomPartitioner.class.getName());
		ConfigHelper.setOutputColumnFamily(conf, Setup.KEYSPACE, Setup.OUTPUT_CF);
		
//		set output class types
		job.setOutputKeyClass(ByteBuffer.class);
		job.setOutputValueClass(List.class);
		job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
//		verbose
		job.waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(), new CassandraWordCount(), args);
		System.exit(0);
	}

}
