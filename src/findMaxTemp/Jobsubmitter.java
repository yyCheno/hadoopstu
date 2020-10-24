package findMaxTemp;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ccyy.ToJar;

public class Jobsubmitter {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		 File jarFile = new ToJar().createTempJar("bin");
			Configuration conf = new Configuration();//实例化，从Hadoop的配置文件里读取参数
			conf.set("mapreduce.app-submission.cross-platform", "true");
			 @SuppressWarnings("deprecation")
			Job job = new Job(conf, "1j-wordcount");//job_name = "wordcount"    
			 job.setJarByClass(Jobsubmitter.class);//输入    

		        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());

			 job.setMapperClass(FindMaxTempMapper.class);  //输入  
			 job.setReducerClass(FindMaxTempReducer.class);   //输入
			 job.setOutputKeyClass(Text.class);    //输出
			 job.setOutputValueClass(IntWritable.class); //输出   
			 FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/1j/input-03/a-03.txt"));//输入文件，端口默认9000，具体看集群配置    
			 FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/1j/output-0604"));//输出文件    
			 System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}

}
