package v1;
//1.1 Read Files from the distribute chache 
//1.2 load all the centers into the mCenters list
//1.3 We split the datapoints and retrieve the values we get the x, y dimensions
//1.4 We split the centers and we get the centerKey, and x,y dimensions
//1.5 We calculate the distance for each datapoint to each center
//1.6 If the distance is smaller that the initial, we assign the following values to the variables
//1.7 we emit the centerkey and the dimensions as a key,  and the datapoint as a value
//1.8 for example C1:1,1	1.987,0.123
//2.1 we loop through all the values and find the cummulative x,y dimensions
//2.2 we calculate the new center
//2.3 We emit as the key the centerKey (which was passed from our map), and the new center
//2.4 For example C1	1.0987,-1.2334 . The reason we need the centerKey is to safely sort the new and the old centers, and compare them correctly
//3.1 we create two paths, the input and the output path
//3.1 we iterate through the algorithm, until the differnce between the centers is smaller than 0,01
//3.2 According to the number of the iteration we load into cache the appropriate file that contains the centers
//3.3 If the iteration is 0, the correct file is centers.txt 
//3.3 If the iteration is 1 the correct folder is the hdfs://quickstart.cloudera/user/cloudera/KMeans/output0
//3.4 If the iteration is i the correct folder is the hdfs://quickstart.cloudera/user/cloudera/KMeans/output + i-1
//3.5 upload the file to hdfs. Overwrite any existing copy.
//3.6 we set up the configuration for the job
//3.7 we create the input and the input files
//3.8 we run the job
//3.9 we open the file that contains the new centers
//3.10 We put the new centers in a ist 
//3.11 we open the file that contains the previous centers
//3.12 We sort the two lists using the center key
//3.13 for each pair of centers, we calulate the distance between the new and the old
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

public class KMeans {
	
	public static List<String> mCenters = new ArrayList<String>();

	
	
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void configure(JobConf job) {
			try {
				//1.1 Read Files from the distribute chache 
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (cacheFiles != null && cacheFiles.length > 0) {
					String line;
					mCenters.clear();
					BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
					try {
						//1.2 load all the centers into the mCenters list
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
		
			//1.3 We split the datapoints and retrieve the values we get the x, y dimensions
			String point = value.toString();
			String[] pointAr = point.split(",");
			Double pointX = Double.parseDouble(pointAr[0]);
			Double pointY = Double.parseDouble(pointAr[1]);
			
			//1.4 We split the centers and we get the centerKey, and x,y dimensions
			String[] center = mCenters.get(0).split("(\t)|(,)");
			String centerKey = center[0];
			Double centerX = Double.parseDouble(center[1]);
			Double centerY = Double.parseDouble(center[2]);
			//1.5 We calculate the distance for each datapoint to each center
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
				//1.6 If the distance is smaller that the initial, we assign the following values to the variables
				if (min>dis){
					min=dis;
					strCenterKey = center[0];
					strCenterX =  center[1];
					strCenterY =  center[2];
				
				}
				
				
			}
			// 1.7 we emit the centerkey and the dimensions as a key,  and the datapoint as a value
			// 1.8 for example C1:1,1	1.987,0.123
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
			
			double pointX = 0;
			double pointY = 0;
			double sumX = 0;
			double sumY = 0;
			int counter = 0;
			//2.1 we loop through all the values and find the cummulative x,y dimensions
			while(values.hasNext()){
				String[] points = values.next().toString().split(",");
				pointX = Double.parseDouble(points[0]);
				pointY = Double.parseDouble(points[1]);
				sumX = sumX + pointX;
				sumY = sumY + pointY;
				counter++;
				
			}
			//2.2 we calculate the new center
			double	newCenterX = sumX/counter;
			double	newCenterY = sumY/counter;
			String newCenter = Double.toString(newCenterX) + "," + Double.toString(newCenterY);
			
			String center[] = key.toString().split(":");
			String centerKey = center[0];
			//2.3 We emit as the key the centerKey (which was passed from our map), and the new center
			//2.4 For example C1	1.0987,-1.2334 . The reason we need the centerKey is to safely sort the new and the old centers, and compare them correctly
			output.collect(new Text(centerKey), new Text(newCenter));
		}
	}
	

	public static void main(String[] args) throws Exception {
		run();
	}

	public static void run() throws Exception {
		
		//3.1 we create two paths, the input and the output path
		String input = "hdfs://quickstart.cloudera/user/cloudera/KMeans/input/";
		String output = "hdfs://quickstart.cloudera/user/cloudera/KMeans/output";
		

		// 3.1 we iterate through the algorithm, until the differnce between the centers is smaller than 0,01
		int iteration = 0;
		boolean isdone = false;
		while (isdone == false) {
			JobConf conf = new JobConf(KMeans.class);
			//3.2 According to the number of the iteration we load into cache the appropriate file that contains the centers
			//3.3 If the iteration is 0, the correct file is centers.txt 
			//3.3 If the iteration is 1 the correct folder is the hdfs://quickstart.cloudera/user/cloudera/KMeans/output0
			//3.4 If the iteration is i the correct folder is the hdfs://quickstart.cloudera/user/cloudera/KMeans/output + i-1
			if (iteration == 0) {
				Path hdfsPath = new Path(input + "centers.txt");
				//3.5 upload the file to hdfs. Overwrite any existing copy.
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			} else {
				Path hdfsPath = new Path(output + Integer.toString(iteration - 1) + "/part-00000");
				//3.5 upload the file to hdfs. Overwrite any existing copy.
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			}

			//3.6 we set up the configuration for the job
			conf.setJobName("KMeans");
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			//3.7 we create the input and the input files
			FileInputFormat.setInputPaths(conf,
					new Path(input + "points.txt"));
			FileOutputFormat.setOutputPath(conf, new Path(output+Integer.toString(iteration)));

			//3.8 we run the job
			JobClient.runJob(conf);

			Configuration configuration = new Configuration();
			configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			FileSystem hdfsFileSystemNew = FileSystem.get(configuration);
			//3.9 we open the file that contains the new centers
			Path ofileNew = new Path("hdfs://quickstart.cloudera/user/cloudera/KMeans/output" +Integer.toString(iteration)+"/part-00000");
//			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					hdfsFileSystemNew.open(ofileNew)));
			
			
			List<String> newCentersList = new ArrayList<String>();
			String line = br.readLine();
			//3.10 We put the new centers in a ist 
			while (line != null) {
				newCentersList.add(line);
				line = br.readLine();
			}
			br.close();
			
			//3.11 we open the file that contains the previous centers
			String previousCenters;
			if (iteration == 0) {
				previousCenters = input + "centers.txt";
			} else {
				previousCenters = output + Integer.toString(iteration - 1)+"/part-00000";
			}
			
			Path ofilePrev = new Path(previousCenters);
			FileSystem hdfsFileSystemPrev = FileSystem.get(configuration);
			BufferedReader br2 = new BufferedReader(new InputStreamReader(
					hdfsFileSystemPrev.open(ofilePrev)));
			List<String> prevCenterList = new ArrayList<String>();
			String line2 = br2.readLine();
			
			
			while (line2 != null) {	
				prevCenterList.add(line2);
				line2 = br2.readLine();
			}
			
			br2.close();
			
		
			

			// 3.12 We sort the two lists using the center key
			Collections.sort(newCentersList);
			Collections.sort(prevCenterList);
			String[] splittedNew = new String[3];
			String[] splittedPrev = new String[3];
			double newX;
			double newY;
			double prevX;
			double prevY;
			Iterator<String> it1 = prevCenterList.iterator();
			for (String it2 : newCentersList) {
				splittedNew = it2.split("(\t)|(,)");
				newX = Double.parseDouble(splittedNew[1]);
				newY = Double.parseDouble(splittedNew[2]);
				String temp = it1.next();
				splittedPrev = temp.split("(\t)|(,)");
				prevX = Double.parseDouble(splittedPrev[1]);
				prevY = Double.parseDouble(splittedPrev[2]);
				

			//3.13 for each pair of centers, we calulate the distance between the new and the old
				if (Math.abs(newX - prevX) <= 0.01 & Math.abs(newY - prevY) <= 0.01) {
					isdone = true;
					
				} else {
					isdone = false;
					break;
				}
				
			}
			
			++iteration;
		}
	}
}