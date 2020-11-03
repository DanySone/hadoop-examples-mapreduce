package com.opstty.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

public class Sort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Map<String, String> dict_trees = new HashMap<String, String>();
        for (String mot : otherArgs) {
            System.out.println(mot);
        }
        if (otherArgs.length > 0) {
            System.err.println("Usage: max height of trees  // No need of argument");
            System.exit(2);
        }
        try {
            Path pt = new Path("trees.csv");//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();
            line = br.readLine();
            while (line != null) {
                String[] arrOfStr = line.split(";", 8);
                if (!arrOfStr[6].isEmpty()) {
                    if (dict_trees.containsKey(arrOfStr[6])) {
                        dict_trees.put(arrOfStr[6], dict_trees.get(arrOfStr[6]) + ", " + arrOfStr[3]);
                    } else {
                        dict_trees.put(arrOfStr[6], arrOfStr[3]);
                    }
                }
                line = br.readLine();
            }
            List<String> height_sort1 = new ArrayList<String>(dict_trees.keySet());
            List<Double> height_sort2 = new ArrayList<Double>();

            for (String s : height_sort1) {
                height_sort2.add(Double.parseDouble(s));
            }
            Collections.sort(height_sort2);

            for (Double d : height_sort2) {
                System.out.println("Height : " + d + " ; Specie(s) : " + dict_trees.get(d));
            }

        } catch (Exception e) {
            System.out.println(e);
        }


    }
}
