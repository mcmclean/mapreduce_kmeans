import java.io.*;

import java.lang.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;

public class standardize extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable> {

	public static String line;
	double[][] mean_stdev = new double[6][2];

	public void configure(JobConf job) {
		Path[] localFiles=new Path[0];
		try{
			localFiles = DistributedCache.getLocalCacheFiles(job);
			BufferedReader fileIn = new BufferedReader(new FileReader(localFiles[0].toString()));
			String line1;
			int index;
			while ((line1=fileIn.readLine()) != null){
				index = Integer.parseInt(line1.substring(0,1));
				line1 = line1.substring(1);
				line1 = line1.replaceAll("\\s+", "");
				String[] token = line1.split(",");
				for(int z = 0; z < 2; z++){
					mean_stdev[index-1][z] = Double.parseDouble(token[z]);
				}
            }
            fileIn.close();
        }
        catch (IOException ioe) {
			 System.err.println("Caught exception while getting cached file: " + StringUtils.stringifyException(ioe));
        }
	}

	protected void setup(OutputCollector<Text, NullWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
		line = value.toString();
		
		String[] values3 = line.split("\t");
		
		if(values3.length > 26){
		
		if(values3[20].matches("\\d+") == true &&
			values3[21].matches("\\d+") == true &&
			values3[22].matches("\\d+") == true &&
			values3[23].matches("\\d+") == true &&
			values3[26].matches("\\d+") == true &&
			values3[26].matches("\\d+") == true){
				
		
		double bene = Double.parseDouble(values3[20]);
		double bene_day = Double.parseDouble(values3[21]);
		double avg_all = Double.parseDouble(values3[22]);
		double std_all = Double.parseDouble(values3[23]);
		double avg_pay = Double.parseDouble(values3[26]);
		double std_pay = Double.parseDouble(values3[27]);
		
		double[] values2 = new double[6];
		
		values2[0] = bene;
		values2[1] = bene_day;
		values2[2] = avg_all;
		values2[3] = std_all;
		values2[4] = avg_pay;
		values2[5] = std_pay;
		
		for(int m =0; m < 6; m++){
			values2[m] = (values2[m] - mean_stdev[m][0])/(mean_stdev[m][1]);
		}
		
		
		String keyval = "";
		for(int p=0; p<6; p++){
			if(p!=5){
				keyval += Double.toString(values2[p]) + ",";
			}
			else{
				keyval += Double.toString(values2[p]) + System.lineSeparator();
			}
		}
		output.collect(new Text(keyval), NullWritable.get());
		}
		}
	}

	protected void cleanup(OutputCollector<Text, NullWritable> output) throws IOException, InterruptedException {
	}
    }
	
    public static class Reduce extends MapReduceBase implements Reducer<Text, NullWritable, Text, NullWritable> {
	
	
	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, NullWritable> output) throws IOException, InterruptedException {
	}

	public void reduce(Text key, Iterator<NullWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
		output.collect(key, NullWritable.get());
	}

	protected void cleanup(OutputCollector<Text, NullWritable> output) throws IOException, InterruptedException {
	}
	}
    

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), standardize.class);
	conf.setJobName("standardize");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(NullWritable.class);

	DistributedCache.addCacheFile(new Path("/user/mmclean/testdir/SDfinal.txt").toUri(),conf);
	
	conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	

	
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new standardize(), args);
	System.exit(res);
    }

}