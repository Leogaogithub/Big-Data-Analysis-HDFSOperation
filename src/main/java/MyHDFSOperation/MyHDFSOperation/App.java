package MyHDFSOperation.MyHDFSOperation;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void upload(String localSrc, String dst) throws Exception{		 
		InputStream in;
		in = new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();				
		//conf.set("fs.default.name", "hdfs://localhost:9000");
	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

	    FileSystem fs = FileSystem.get(URI.create(dst), conf);
	    OutputStream out = fs.create(new Path(dst), new Progressable() {
	      public void progress() {
	        System.out.print(".");
	      }
	    });	    
	    IOUtils.copyBytes(in, out, 4096, true); 
	    System.out.println("we have upload the file to " + dst);
	}
	
	public static void decompress(String uri) throws Exception{	    
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    
	    Path inputPath = new Path(uri);
	    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	    CompressionCodec codec = factory.getCodec(inputPath);
	    if (codec == null) {
	      System.err.println("No codec found for " + uri);
	      System.exit(1);
	    }

	    String outputUri =
	      CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

	    InputStream in = null;
	    OutputStream out = null;
	    try {
	      in = codec.createInputStream(fs.open(inputPath));
	      out = fs.create(new Path(outputUri));
	      IOUtils.copyBytes(in, out, conf);
	    } finally {
	      IOUtils.closeStream(in);
	      IOUtils.closeStream(out);
	    }
	    System.out.println(uri+" has been decompressed !");
	}
	
	public static void delete(String uri) throws Exception{
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);	    
	    Path inputPath = new Path(uri);
	    fs.delete(inputPath, false);
	    System.out.println(uri + " is deleted!");
	}
	
	public static void deleteLocal(String localName){
		 File f = new File(localName);         
         f.delete();
         System.out.println("local file: "+localName + " is deleted!");
	}
	public static void download(String address, String dir) {
        OutputStream out = null;
        URLConnection conn = null;
        InputStream in = null;
        String localFileName=address.substring(address.lastIndexOf("/")+1);
        String pathName = dir+"/"+localFileName;
        try {
            URL url = new URL(address);
            out = new BufferedOutputStream(new FileOutputStream(pathName));
            conn = url.openConnection();
            in = conn.getInputStream();
            byte[] buffer = new byte[1024];

            int numRead;
            long numWritten = 0;

            while ((numRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, numRead);
                numWritten += numRead;
            }
            System.out.println("we have downloaded the file " + address + " and put it in "+pathName + "\t" + numWritten);
        } 
        catch (Exception exception) { 
            exception.printStackTrace();
        } 
        finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
            } 
            catch (IOException ioe) {
            }
        }
    }
	
	public static void main(String[] args) throws Exception{
		//download	the	following	books
		//String localDir = "/home/leo";
		//String dstDir = "hdfs://localhost:9000/user/leo";
		String localDir = ".";		
		String dstDir = "hdfs://cshadoop1/user/lxg151530/assignment1";
		
		ArrayList<String> downloadList = new ArrayList<String>();
		downloadList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2");
		downloadList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2");
		downloadList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2");
		downloadList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2");
		downloadList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2");
		downloadList.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2");
		
		for(String downloadFileName : downloadList){
			String localFileName=downloadFileName.substring(downloadFileName.lastIndexOf("/")+1);		
			download(downloadFileName, localDir);
			upload(localDir+"/"+localFileName, dstDir+"/"+localFileName);
			decompress(dstDir+"/"+localFileName);
			delete(dstDir+"/"+localFileName);
			deleteLocal(localDir+"/"+localFileName);
		}
	}
}
