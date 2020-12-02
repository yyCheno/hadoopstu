package ccyy;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class JobSubmitter {

	public static void main(String[] args) throws Exception{

        File jarFile = createTempJar("bin");
		Configuration conf = new Configuration();//实例化，从Hadoop的配置文件里读取参数
		conf.set("mapreduce.app-submission.cross-platform", "true");
		 @SuppressWarnings("deprecation")
		Job job = new Job(conf, "1j-wordcount");//job_name = "wordcount"    
		 job.setJarByClass(JobSubmitter.class);//输入    

	        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());

		 job.setMapperClass(WordCountMapper.class);  //输入  
		 job.setReducerClass(WordCountReducer.class);   //输入
		 job.setOutputKeyClass(Text.class);    //输出
		 job.setOutputValueClass(IntWritable.class); //输出   
		 FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/1j/input-03/a-03.txt"));//输入文件，端口默认9000，具体看集群配置    
		 FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/1j/output-012"));//输出文件    
		 System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
    public static File createTempJar(String root) throws IOException {
        if (!new File(root).exists()) {
             return new File(System.getProperty("java.class.path"));
        }
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        final File jarFile = File.createTempFile("EJob-", ".jar", new File(System.getProperty("java.io.tmpdir")));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                jarFile.delete();
            }
        });
        JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile), manifest);
        createTempJarInner(out, new File(root), "");
        out.flush();
        out.close();
        return jarFile;
    }
 
    private static void createTempJarInner(JarOutputStream out, File f,
            String base) throws IOException {
        if (f.isDirectory()) {
            File[] fl = f.listFiles();
            if (base.length() > 0) {
                base = base + "/";
            }
            for (int i = 0; i < fl.length; i++) {
                createTempJarInner(out, fl[i], base + fl[i].getName());
            }
        } else {
            out.putNextEntry(new JarEntry(base));
            FileInputStream in = new FileInputStream(f);
            byte[] buffer = new byte[1024];
            int n = in.read(buffer);
            while (n != -1) {
                out.write(buffer, 0, n);
                n = in.read(buffer);
            }
            in.close();
        }
    }

}
