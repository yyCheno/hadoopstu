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
			Configuration conf = new Configuration();//ʵ��������Hadoop�������ļ����ȡ����
			conf.set("mapreduce.app-submission.cross-platform", "true");
			 @SuppressWarnings("deprecation")
			Job job = new Job(conf, "1j-wordcount");//job_name = "wordcount"    
			 job.setJarByClass(Jobsubmitter.class);//����    

		        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());

			 job.setMapperClass(FindMaxTempMapper.class);  //����  
			 job.setReducerClass(FindMaxTempReducer.class);   //����
			 job.setOutputKeyClass(Text.class);    //���
			 job.setOutputValueClass(IntWritable.class); //���   
			 FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/1j/input-03/a-03.txt"));//�����ļ����˿�Ĭ��9000�����忴��Ⱥ����    
			 FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/1j/output-0604"));//����ļ�    
			 System.exit(job.waitForCompletion(true) ? 0 : 1);//��ִ����ϣ��˳�
	}

}
