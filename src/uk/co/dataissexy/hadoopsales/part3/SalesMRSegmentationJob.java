package uk.co.dataissexy.hadoopsales.part3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SalesMRSegmentationJob {

	public static class SalesMRSegmentationJobMapper extends Mapper<Object, Text, Text, NullWritable> {
		
		private Text userid = new Text();
		private Text userinfo = new Text();
		private MultipleOutputs<Text,NullWritable> multi = null;
		
		protected void setup(Context context) {
			multi = new MultipleOutputs<Text,NullWritable>(context);
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			double[] datavalues = new double[12];

			for (int i = 1; i <= 12; i++) {
				datavalues[i - 1] = Double.parseDouble(split[i]);
			}
			Statistics s = new Statistics(datavalues);

			double mean = s.getMean();
			double variance = s.getVariance();
			double salesdrop = calcSalesDrop(Double.parseDouble(split[13]), mean);
			double monthsbelow = monthsBelow(datavalues, mean);
			
			StringBuilder sb = new StringBuilder().append(mean + "\t")
					.append(variance + "\t")
					.append(salesdrop + "\t")
					.append(monthsbelow);
			
			userid.set(split[0]);
			userinfo.set(sb.toString());
			
			// work out our segments 
			// 1. if salesdrop is greater than 5 units
			if(salesdrop > 5) {
				multi.write("segments", userid, userinfo, "salesdrop");
			} else if(monthsbelow > 4){
				multi.write("segments", userid, userinfo, "monthsbelow");
			} else if(mean > 9) {
				multi.write("segments", userid, userinfo, "goodsalestorewards");
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multi.close();
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

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		
		Job job = new Job(config, "SalesMRSegmentJob");
		job.setJarByClass(SalesMRSegmentationJob.class);
		job.setMapperClass(SalesMRSegmentationJobMapper.class);
		job.setNumReduceTasks(0);
		
		
		MultipleOutputs.addNamedOutput(job, "segments", TextOutputFormat.class,
				Text.class, NullWritable.class);

		MultipleOutputs.setCountersEnabled(job, true);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		MultipleOutputs.setCountersEnabled(job, true);
		System.exit(job.waitForCompletion(true) ? 0 : 2);
		
	}

}
