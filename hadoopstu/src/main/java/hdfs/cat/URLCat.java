package hdfs.cat;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class URLCat {
	public URLCat() {
		// TODO Auto-generated constructor stub
	}
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());//
	}
	public void run(String hdfsurl) throws Exception{
		InputStream in = null;
		try {
			in = new URL(hdfsurl).openStream();
			IOUtils.copyBytes(in, System.out, 4096,false);
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			IOUtils.closeStream(in);
		}
	}
}
