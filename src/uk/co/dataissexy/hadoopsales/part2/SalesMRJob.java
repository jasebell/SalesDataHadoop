package uk.co.dataissexy.hadoopsales.part2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.Mapper;


public class SalesMRJob {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		
		private Text userid = new Text();
		private Text userinfo = new Text();
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] split = value.toString().split(",");
			double[] datavalues = new double[12];

			for (int i = 1; i <= 12; i++) {
				datavalues[i - 1] = Double.parseDouble(split[i]);
			}
			Statistics s = new Statistics(datavalues);

			double mean = s.getMean();
			double variance = s.getVariance();
			
			StringBuilder sb = new StringBuilder().append(mean + "\t")
					.append(variance + "\t")
					.append(calcSalesDrop(Double.parseDouble(split[13]), mean))
					.append(monthsBelow(datavalues, mean));
			
			userid.set(split[0]);
			userinfo.set(sb.toString());
			
			output.collect(userid, userinfo);
			
		}

		private double calcSalesDrop(double sales, double mean) {
			return (mean - sales);
		}

		private int monthsBelow(double data[], double mean) {
			int count = 0;
			for (double a : data) {
				if ((a < (mean * 0.40)))
					count++;
			}
			return count;
		}

		private class Statistics {
			double[] data;
			double size;

			public Statistics(double data[]) {
				this.data = data;
				this.size = data.length;
			}

			double getMean() {
				double sum = 0.0;
				for (double a : data) {
					sum += a;
				}
				return sum / size;
			}

			double getVariance() {
				double mean = getMean();
				double temp = 0;
				for (double a : data) {
					temp += (mean - a) * (mean - a);
				}
				return temp / size;
			}
		}

	}

	public static void main(String[] args) {

		JobConf conf = new JobConf(SalesMRJob.class);
		conf.setJobName("SalesMRJob");
		conf.setNumReduceTasks(0);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
