package com.opstty.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class NumberTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Map<String, Integer> dict_trees = new HashMap<String, Integer>();
        for (String mot : otherArgs) {
            System.out.println(mot);
        }
        if (otherArgs.length > 0) {
            System.err.println("Usage: number of trees  // No need of argument");
            System.exit(2);
        }
        try {
            Path pt = new Path("trees.csv");//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();
            line = br.readLine();
            ArrayList<String> arr = new ArrayList<String>();

            while (line != null) {
                String[] arrOfStr = line.split(";", 5);
                arr.add(arrOfStr[3]);
                line = br.readLine();
            }

            for (String s : arr) {

                if (dict_trees.containsKey(s)) {
                    dict_trees.put(s, dict_trees.get(s) + 1);
                } else {
                    dict_trees.put(s, 1);
                }
            }
            for (String specie : dict_trees.keySet()) {
                String nbr = dict_trees.get(specie).toString();
                System.out.println("Specie : " + specie + " ; Number of trees : " + nbr);
            }
        } catch (Exception e) {
        }


    }
}
