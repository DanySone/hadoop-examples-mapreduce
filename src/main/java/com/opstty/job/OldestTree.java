package com.opstty.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Calendar;

public class OldestTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        for (String mot : otherArgs) {
            System.out.println(mot);
        }
        if (otherArgs.length > 0) {
            System.err.println("Usage: oldest tree  // No need of argument");
            System.exit(2);
        }
        try {
            Path pt = new Path("trees.csv");//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            int year = Calendar.getInstance().get(Calendar.YEAR);
            int min_year = Calendar.getInstance().get(Calendar.YEAR);
            int district = -1; //-1 if district does not exist
            String specie = "null"; //null if specie does not exist
            String line;
            line = br.readLine();
            line = br.readLine();
            while (line != null) {
                String[] arrOfStr = line.split(";", 8);
                if (!arrOfStr[5].isEmpty() && !arrOfStr[1].isEmpty() && !arrOfStr[3].isEmpty()) {
                    if (Integer.parseInt(arrOfStr[5]) < min_year) {
                        min_year = Integer.parseInt(arrOfStr[5]);
                        specie = arrOfStr[3];
                        district = Integer.parseInt(arrOfStr[1]);

                    }
                }
                line = br.readLine();
            }

            System.out.println("The oldest tree is a " + specie + " and " + (year - min_year) + " years old in district : " + district);

        } catch (Exception e) {
            System.out.println(e);
        }


    }
}
