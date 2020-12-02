package myhdfs;


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;


public class MyFileSystem {
	private Configuration conf;
	public MyFileSystem() {
		// TODO Auto-generated constructor stub
	}
	public MyFileSystem(Configuration conf) {
		this.setConf(conf);
	}
	public Configuration getConf() {
		return conf;
	}
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	public InputStream cat(String uri) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		in = fs.open(new Path(uri));
		return in;	
	}
	public FSDataInputStream doublecat(String uri) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		in = fs.open(new Path(uri));
		return in;
	}
	public void copyFromLocal(String src,String dst) throws Exception {
		InputStream in = new BufferedInputStream(new FileInputStream(src));
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst),new Progressable() {
			@Override
			public void progress() {
				// TODO Auto-generated method stub
				System.out.print("-");
			}
		});
		IOUtils.copyBytes(in, out, 4096,true);
	}
}
