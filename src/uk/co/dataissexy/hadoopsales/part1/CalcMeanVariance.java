package uk.co.dataissexy.hadoopsales.part1;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class CalcMeanVariance {

	public CalcMeanVariance(){
		try {
			BufferedReader in = new BufferedReader(new FileReader(
					"/Users/Jason/data/salesdata.csv"));
			String str;
			while ((str = in.readLine()) != null) {
				String[] split = str.split(",");
				double[] values = new double[12];
				// 0 = userid
				// 1 - 13 = months sales. Jan - Jan, Mar - Mar etc
				for (int i = 1; i <= 12; i++) {
					values[i-1] = Double.parseDouble(split[i]);
				}
				Statistics s = new Statistics(values);
				
				double mean = s.getMean();
				double variance = s.getVariance();
				System.out.println("User id: " + split[0]);
				System.out.println("\tMean: " + mean);
				System.out.println("\tVariance: " + variance);
				System.out.println("\tMonth 13 Sales Drop = " + calcSalesDrop(Double.parseDouble(split[13]), mean));
				System.out.println("\tMonths 40% below mean: " + monthsBelow(values, mean));
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private double calcSalesDrop(double sales, double mean) {
		return (mean - sales);
	}
	
	private int monthsBelow(double data[], double mean) {
		int count = 0;
		for(double a : data){
			if((a < (mean * 0.40))) count++;
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
			for(double a : data) {
				sum += a;
			}
			return sum/size;
		}
		
		double getVariance() {
			double mean = getMean();
			double temp = 0;
			for(double a : data) {
				temp += (mean - a)*(mean - a);
			}
			return temp/size;
		}
	}
	
	
	public static void main(String[] args) {
		CalcMeanVariance cvm = new CalcMeanVariance();

	}

}
