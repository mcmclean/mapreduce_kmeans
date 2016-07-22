import java.io.*;

import java.lang.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SDMean extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	public static String line;
	public static double bene;
	public static double bene_day;
	
	public static double avg_all;
	public static double std_all;
	public static double avg_pay;
	public static double std_pay;

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	   //int centroid;
		line = value.toString();
		//String line1 = line.replaceAll("\\s+", "");
		//line1 = line1.replaceAll(".", "");
		//line1 = line1.replaceAll("-", "");
		//line1 = line1.replaceAll(",", "");
		//line1 = line1.replaceAll("[a-zA-Z]", "");
		
		//if(line1.matches("\\d+")){
		
		//if(line != null){
		
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
		
		output.collect(new Text("1"), new DoubleWritable(bene));
		output.collect(new Text("2"), new DoubleWritable(bene_day));
		output.collect(new Text("3"), new DoubleWritable(avg_all));
		output.collect(new Text("4"), new DoubleWritable(std_all));
		output.collect(new Text("5"), new DoubleWritable(avg_pay));
		output.collect(new Text("6"), new DoubleWritable(std_pay));
		}
		//}
		}
	}

	protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}
    }
	
    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, Text> {
	
	public double sum;
	public double sumsquare;
	public double count;
	public double avg;
	public double avgsquare;
	public double sd;
	public double current;
	
	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
	}

	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    sum = 0;
		sumsquare = 0;
		count = 0;
		while (values.hasNext()) {
			current = values.next().get();
			sum += current;
			sumsquare += (current*current);
			count += 1;
	    }
		avg = sum / count;
		avgsquare = sumsquare / count;
		
		sd = Math.sqrt(avgsquare - avg*avg);
		
		String reduceval = Double.toString(avg) + "," + Double.toString(sd);
		    
		output.collect(key, new Text(reduceval));
		
	}

	protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), SDMean.class);
	conf.setJobName("SDMean");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

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
	int res = ToolRunner.run(new Configuration(), new SDMean(), args);
	System.exit(res);
    }
}
