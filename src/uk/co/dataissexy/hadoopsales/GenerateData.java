package uk.co.dataissexy.hadoopsales;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class GenerateData {

	public GenerateData() {
		Random rand = new Random();
		StringBuilder sb = new StringBuilder();
		for (int i = 1; i <= 2000000; i++) {
			sb.append(i + ",");
			for (int j = 1; j <= 13; j++) {
				sb.append(rand.nextInt(15));
				if(j == 13) {
					sb.append("\n");
				} else {
					sb.append(",");
				}
			}
		}

		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(
					"/Users/Jason/bigsalesdata.csv"));
			out.write(sb.toString());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		GenerateData gd = new GenerateData();

	}

}
