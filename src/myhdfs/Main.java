package myhdfs;

import org.apache.hadoop.conf.Configuration;

public class Main {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		MyFileSystem fs = new MyFileSystem(conf);
		fs.copyFromLocal("D:/Python���ݿ�ѧ�ֲ᡾�������İ桿.pdf", "hdfs://master:9000/1j/orther/Python���ݿ�ѧ�ֲ᡾�������İ桿.pdf");
	}

}
