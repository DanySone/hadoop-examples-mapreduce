package com.opstty.job;

import com.fasterxml.jackson.databind.util.TypeKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MaxHeightTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Map<String, Double> dict_trees = new HashMap<String, Double>();
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
                    if (dict_trees.containsKey(arrOfStr[3])) {
                        if (dict_trees.get(arrOfStr[3]) < Double.parseDouble(arrOfStr[6])) {
                            dict_trees.put(arrOfStr[3], Double.parseDouble(arrOfStr[6]));
                        }

                    } else {
                        dict_trees.put(arrOfStr[3], Double.parseDouble(arrOfStr[6]));
                    }
                }
                line = br.readLine();
            }

            for (String specie : dict_trees.keySet()) {
                String height = dict_trees.get(specie).toString();
                System.out.println("Specie : " + specie + " ; Height : " + height);
            }

        } catch (Exception e) {
            System.out.println(e);
        }


    }
}
