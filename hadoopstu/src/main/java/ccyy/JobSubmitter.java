package ccyy;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class JobSubmitter {

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();//实例化，从Hadoop的配置文件里读取参数
		
		 @SuppressWarnings("deprecation")
		Job job = new Job(conf, "wordcount");//job_name = "wordcount"    
		 job.setJarByClass(JobSubmitter.class);//输入    
		 job.setMapperClass(WordCountMapper.class);  //输入  
		 job.setReducerClass(WordCountReducer.class);   //输入
		 job.setOutputKeyClass(Text.class);    //输出
		 job.setOutputValueClass(IntWritable.class); //输出   
		 FileInputFormat.addInputPath(job, new Path("hdfs://118.178.92.103:9000/1j/input-03/a-03.txt"));//输入文件    
		 FileOutputFormat.setOutputPath(job, new Path("hdfs://118.178.92.103:9000/1j/output-05"));//输出文件    
		 System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}

}
