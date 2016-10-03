package v1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Random;
import java.util.Scanner;

public class Skewed {

	public static void main(String[] args) throws IOException {
	
		BufferedWriter writer = null;
		BufferedWriter writerC = null;
        try {
            //1.create a temporary file
            String points = "points.txt";
            File logFileP = new File(points);
            
            String centers = "centers.txt";
            File logFileC = new File(centers);
            
            
            
            // 2.This will output the full path where the file will be written to...
            writer = new BufferedWriter(new FileWriter(logFileP));
            writerC = new BufferedWriter(new FileWriter(logFileC));
            
         
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            
		createPoint(writer, writerC);
		writer.close();
		writerC.close();
        }
    }
	
	public static void createPoint(BufferedWriter writer, BufferedWriter writerC)throws IOException{
		 Scanner in = new Scanner(System.in);
		 
		 System.out.println("Please insert x value for center 1");
		 
		 double xCenter1 = in.nextDouble();
		 
		 String strXCenter1 = Double.toString(xCenter1);
		 System.out.println("Please insert y value for center 1");
		 double yCenter1 = in.nextDouble();
		 in.nextLine();
		 String strYCenter1 = Double.toString(yCenter1);
		 writerC.write("C1\t" + strXCenter1 + "," + strYCenter1);
		 writerC.newLine();
		 
		 
		 
		 
		 System.out.println("Please insert x value for center 2");
		 double xCenter2 = in.nextDouble();
		 String strXCenter2 = Double.toString(xCenter2);
		 in.nextLine();
		 System.out.println("Please insert y value for center 2");
		 double yCenter2 = in.nextDouble();
		 in.nextLine();
		 String strYCenter2 = Double.toString(yCenter2);
		 writerC.write("C2\t" + strXCenter2 + "," + strYCenter2);
		 writerC.newLine();
		 
		 
		 System.out.println("Please insert x value for center 3");
		 double xCenter3 = in.nextDouble();
		 in.nextLine();
		 String strXCenter3 = Double.toString(xCenter3);
		 System.out.println("Please insert y value for center 3");
		 double yCenter3 = in.nextDouble();
		 in.nextLine();
		 String strYCenter3 = Double.toString(yCenter3);
		 writerC.write("C3\t" + strXCenter3 + "," + strYCenter3);
		 writerC.newLine();
		 writerC.close();
		 
		
		int stddev = 20;
		Random r = new Random();
		
		for (int i = 0; i<350000; i++){
			double x1  = Math.max(xCenter1-100, Math.min(xCenter1+100, (int) xCenter1  + r.nextGaussian() * stddev));
			double y1  = Math.max(yCenter1-100, Math.min(yCenter1+100, (int) yCenter1 + r.nextGaussian() * stddev));
			String xp1 = String.format("%.2f", x1);
			String yp1 = String.format("%.2f", y1);
			writer.write(xp1 + "," + yp1);
			writer.newLine();
			
			double x2  = Math.max(xCenter2-100, Math.min(xCenter2+100, (int) xCenter2  + r.nextGaussian() * stddev));
			double y2  = Math.max(yCenter2-100, Math.min(yCenter2+100, (int) yCenter2 + r.nextGaussian() * stddev));
			String xp2 = String.format("%.2f", x2);
			String yp2 = String.format("%.2f", y2);
			writer.write(xp2 + "," + yp2);
			writer.newLine();
			
			double x3  = Math.max(xCenter3-100, Math.min(xCenter3+100, (int) xCenter3  + r.nextGaussian() * stddev));
			double y3  = Math.max(yCenter3-100, Math.min(yCenter3+100, (int) yCenter3 + r.nextGaussian() * stddev));
			String xp3 = String.format("%.2f", x3);
			String yp3 = String.format("%.2f", y3);
			writer.write(xp3 + "," + yp3);
			writer.newLine();
		}
		writer.close();
	}
	

}
