package com.opstty.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

public class MostTree {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Map<String, Integer> dict_trees = new HashMap<String, Integer>();
        Map.Entry<String, Integer> max_entry = null;
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
                if (!arrOfStr[1].isEmpty()) {
                    if (dict_trees.containsKey(arrOfStr[1])) {
                        dict_trees.put(arrOfStr[1], dict_trees.get(arrOfStr[1]) + 1);
                    } else {
                        dict_trees.put(arrOfStr[1], 1);
                    }
                }
                line = br.readLine();
            }

            for (Map.Entry<String, Integer> trees_entry : dict_trees.entrySet()) {
                if (max_entry == null || trees_entry.getValue() > max_entry.getValue()) {
                    max_entry = trees_entry;

                }
            }
            String max_district = max_entry.getKey();
            int max_nbr_trees = max_entry.getValue();
            System.out.println("The district with the most trees is " + max_district + " with " + max_nbr_trees + " trees.");

        } catch (Exception e) {
            System.out.println(e);
        }


    }
}
