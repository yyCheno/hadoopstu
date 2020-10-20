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
		
		Configuration conf = new Configuration();//ʵ��������Hadoop�������ļ����ȡ����
		
		 @SuppressWarnings("deprecation")
		Job job = new Job(conf, "wordcount");//job_name = "wordcount"    
		 job.setJarByClass(JobSubmitter.class);//����    
		 job.setMapperClass(WordCountMapper.class);  //����  
		 job.setReducerClass(WordCountReducer.class);   //����
		 job.setOutputKeyClass(Text.class);    //���
		 job.setOutputValueClass(IntWritable.class); //���   
		 FileInputFormat.addInputPath(job, new Path("hdfs://118.178.92.103:9000/1j/input-03/a-03.txt"));//�����ļ�    
		 FileOutputFormat.setOutputPath(job, new Path("hdfs://118.178.92.103:9000/1j/output-05"));//����ļ�    
		 System.exit(job.waitForCompletion(true) ? 0 : 1);//��ִ����ϣ��˳�
	}

}
