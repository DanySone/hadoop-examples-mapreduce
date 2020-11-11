package com.opstty.job;

import com.opstty.mapper.TokenizerMapper;
import com.opstty.reducer.IntSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class District {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        for (String mot : otherArgs) {
            System.out.println(mot);
        }
        if (otherArgs.length > 0) {
            System.err.println("Usage: district  // No need of argument");
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
                String[] arrOfStr = line.split(";", 3);
                if (!arr.contains(arrOfStr[1])) {
                    arr.add(arrOfStr[1]);
                }
                line = br.readLine();
            }
            System.out.println("The districts that contain trees are " + arr + " Total = " + arr.size() + " districts");
        } catch (Exception e) {
        }


    }
}