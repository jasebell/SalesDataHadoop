package uk.co.dataissexy.hadoopsales.part2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SalesMapper {
	public static class SalesMapperContext implements Mapper<Object, Text, Text, IntWritable> {

		

		@Override
		public void configure(JobConf arg0) {
			
		}

		@Override
		public void close() throws IOException {
			
		}

		@Override
		public void map(Object arg0, Text arg1,
				OutputCollector<Text, IntWritable> arg2, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			
		}
	}
}