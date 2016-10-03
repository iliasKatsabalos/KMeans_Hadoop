package v2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.Progressable;

public class KMeans2 {
	//1.1 mCenters stores all the cendroids from the cachefile 
	public static List<String> mCenters = new ArrayList<String>();

	
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void configure(JobConf job) {
			try {
				//1.1 mCenters stores all the cendroids from the cachefile 
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (cacheFiles != null && cacheFiles.length > 0) {
					String line;
					mCenters.clear();
					BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
					try {
						while ((line = cacheReader.readLine()) != null) {
							mCenters.add(line);
							System.out.println(line);
						}
					} finally {
						cacheReader.close();
					}
				}
			} catch (IOException e) {
				System.err.println("Exception reading DistribtuedCache: " + e);
			}
		}

		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			//1.2 Split the string point and store x and y in two different variables pointX,pointY
			String point = value.toString();
			String[] pointAr = point.split(",");
			Double pointX = Double.parseDouble(pointAr[0]);
			Double pointY = Double.parseDouble(pointAr[1]);
			
			// 1.3 Calculate the distance from all the centers and store the nearest center 
			
			String[] center = mCenters.get(0).split("(\t)|(,)");
			String centerKey = center[0];
			Double centerX = Double.parseDouble(center[1]);
			Double centerY = Double.parseDouble(center[2]);
			Double dis =Math.sqrt(Math.pow(centerX-pointX, 2) + Math.pow(centerY-pointY, 2));
			Double min = dis;
			String strCenterX = center[1];
			String strCenterY = center[2];
			String strCenterKey = center[0];
			for (int i=1;i<mCenters.size();i++) {
				center = mCenters.get(i).split("(\t)|(,)");
				centerKey = center[0];
				centerX = Double.parseDouble(center[1]);
				centerY = Double.parseDouble(center[2]);
				dis = Math.sqrt(Math.pow(centerX-pointX, 2) + Math.pow(centerY-pointY, 2));
				if (min>dis){
					min=dis;
					strCenterKey = center[0];
					strCenterX =  center[1];
					strCenterY =  center[2];
				
				}
				
				
			}
			// 1.4 Add the nearest center and the point in the list 
			output.collect(new Text(strCenterKey + ":" + strCenterX + "," + strCenterY),
					new Text(point));
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text , Text > {

			
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Configuration conf = new Configuration();
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			FileSystem hdfs = FileSystem.get(conf);
			//2.1 Create a output path and a file on it
			Path file = new Path("hdfs://quickstart.cloudera/user/cloudera/KMeans2/output/" + key.toString().substring(0, 2));
			OutputStream os = hdfs.create(file);
			BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
						
					
				
			
			double pointX = 0;
			double pointY = 0;
			double sumX = 0;
			double sumY = 0;
			int counter = 0;
			String line = "";
			while(values.hasNext()){
				
				//2.2 write all the points into the created file 
				//Each reduce job matches one cendroid, as a result there will be created 3 files with the points of each center.
				line = values.next().toString();
				String[] points = line.split(",");
				pointX = Double.parseDouble(points[0]);
				pointY = Double.parseDouble(points[1]);
				sumX = sumX + pointX;
				sumY = sumY + pointY;
				
				br.write(line);
				br.newLine();
				
				counter++;
				
			}
			double	newCenterX = sumX/counter;
			double	newCenterY = sumY/counter;
			String newCenter = Double.toString(newCenterX) + "," + Double.toString(newCenterY);
			
			String center[] = key.toString().split(":");
			String centerKey = center[0];
			// Emit new center and point
			br.close();
			hdfs.close();
			output.collect(new Text(centerKey), new Text(newCenter));
			
		}
	}
	

	public static void main(String[] args) throws Exception {
		run();
	}

	public static void run() throws Exception {
		
		String input = "hdfs://quickstart.cloudera/user/cloudera/KMeans/input/";
		String output = "hdfs://quickstart.cloudera/user/cloudera/KMeans2/output/";
		

		
		int iteration = 0;
		boolean isdone = false;
		//3.1 Find how many ouput files have been created so we can store the path with the last centers.
		Path ofileNew = new Path("hdfs://quickstart.cloudera/user/cloudera/KMeans/output0/part-00000");
		while (isdone == false) {
			
			
			Configuration configuration = new Configuration();
			configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			FileSystem lastJob = FileSystem.get(configuration);
			ofileNew = new Path("hdfs://quickstart.cloudera/user/cloudera/KMeans/output" +Integer.toString(iteration)+"/part-00000");
			if(!lastJob.exists(ofileNew)){
				break;
			}
			
			
			++iteration;
		}
		iteration = iteration - 1;
		
		//3.2 Map reduce job is running for the final centers
		JobConf conf = new JobConf(KMeans2.class);
		

		conf.setJobName("KMeans");
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		

		DistributedCache.addCacheFile(new Path("hdfs://quickstart.cloudera/user/cloudera/KMeans/output" +Integer.toString(iteration)+"/part-00000").toUri(), conf);

		FileInputFormat.setInputPaths(conf,
				new Path(input + "points.txt"));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);

		
					

	}
}
