package ccyy;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException; 
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	IntWritable one = new IntWritable(1);  //�������ֵʼ����1
	Text word = new Text();  //���������ʽkey����ʽ
	public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());//����ֵ��Text����Ҫת��
		Log log = LogFactory.getLog(WordCountMapper.class);
		log.error("error");
		System.err.println("err");
		while (itr.hasMoreTokens()){      
			word.set(itr.nextToken()); //������Ŀ��Ա�������
			//System.out.printf("%d\n",one);
			context.write(word, one); //����ʽ<word,1>����  
		}  
 
}
}