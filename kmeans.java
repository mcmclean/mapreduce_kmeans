import java.io.*;

import java.lang.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.util.*;

public class kmeans extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

    public static String line;
	public static double[][] clusters;
	public static int numClusters;
	
	public static String val;
	public double distance;
	public double mindistance = 0;
	public int centroid;
	
	public void configure(JobConf job) {
		
		numClusters = Integer.parseInt(job.get("numClusters"));
		
		Path[] localFiles=new Path[0];
		try{
			localFiles = DistributedCache.getLocalCacheFiles(job);
			BufferedReader fileIn = new BufferedReader(new FileReader(localFiles[0].toString()));
			String line1;
			int count1 = 0;
			clusters = new double[numClusters][6];
			while ((line1=fileIn.readLine()) != null){
				String[] token = line1.split("\\s+");
				if(count1 < numClusters){
					for(int z = 0; z < 6; z++){
						clusters[count1][z] = Double.parseDouble(token[z]);
					}
				}
				count1++;
            }
            fileIn.close();
        }
        catch (IOException ioe) {
			 System.err.println("Caught exception while getting cached file: " + StringUtils.stringifyException(ioe));
        }
	}

	protected void setup(OutputCollector<IntWritable, Text> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		line = value.toString();
		line = line.trim();
		
		if(line != null && line.length()!= 0 ){
		
		String[] values3 = line.split(",");
		
		double bene = Double.parseDouble(values3[0]);
		double bene_day = Double.parseDouble(values3[1]);
		double avg_all = Double.parseDouble(values3[2]);
		double std_all = Double.parseDouble(values3[3]);
		double avg_pay = Double.parseDouble(values3[4]);
		double std_pay = Double.parseDouble(values3[5]);
		
		double[] values2 = new double[6];
		
		values2[0] = bene;
		values2[1] = bene_day;
		values2[2] = avg_all;
		values2[3] = std_all;
		values2[4] = avg_pay;
		values2[5] = std_pay;
		
		double[] distance = new double[numClusters];
		
		for(int i = 0; i<numClusters; i++){
			for(int j = 0; j < 6; j++){
				distance[i] += (Math.pow((clusters[i][j] - values2[j]),2));
			}
		}
		
		for(int q = 0; q < 6; q++){
			val += (values3[q]) + ", ";
		}
		
		for(int r=0; r<numClusters;r++){
			if(r == 0){
				mindistance = distance[r];
				centroid = r;
			}
			else if(mindistance >= distance[r]){
				mindistance = distance[r];
				centroid = r;
			}
		}
		
		output.collect(new IntWritable(centroid), new Text(val));
		
		}
	}

	protected void cleanup(OutputCollector<IntWritable, Text> output) throws IOException, InterruptedException {
	}
    }
	
    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
	
	public static String cent = "";
	public String curr;
	public double[] vals;
	public int len;
	public String[] vec1;
	
	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<IntWritable, Text> output) throws IOException, InterruptedException {
	}

	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		
		while (values.hasNext()) {
			curr = values.next().toString();
			curr = curr.replaceAll("null", " ");
			curr = curr.trim();
			
			
			String[] vec = curr.split(",");
			len = vec.length;
			
			vec1 = vec;
			
			vals = new double[6];
			
			for(int z = 0; z < vec1.length; z++){
				if(z%6 == 0){vals[0] += Double.parseDouble(vec1[z]);}
				else if(z%6 == 1){vals[1] += Double.parseDouble(vec1[z]);}
				else if(z%6 == 2){vals[2] += Double.parseDouble(vec1[z]);}
				else if(z%6 == 3){vals[3] += Double.parseDouble(vec1[z]);}
				else if(z%6 == 4){vals[4] += Double.parseDouble(vec1[z]);}
				else if(z%6 == 5){vals[5] += Double.parseDouble(vec1[z]);}
			}
			
	    }
		
		for(int l = 0; l < 6; l++){
			vals[l] = vals[l] / len;
		}
		
		for(int s = 0; s < 6; s++){
			cent += Double.toString(vals[s]) + ", ";
		}
		
		output.collect(key, new Text(cent));
		
	}

	protected void cleanup(OutputCollector<IntWritable, Text> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), kmeans.class);
	conf.setJobName("kmeans");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(Text.class);
	
	//DISTRIBUTED CACHE
	DistributedCache.addCacheFile(new Path("/user/mmclean/testdir/centroids.txt").toUri(),conf);

	conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);
	
	conf.set("numClusters",args[2]);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new kmeans(), args);
	System.exit(res);
    }
}
