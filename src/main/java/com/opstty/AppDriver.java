package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("district", District.class,
                    "A map/reduce program that lists the different districts that contains trees.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that lists all different species in this file.");
            programDriver.addClass("numbertree", NumberTree.class,
                    "A map/reduce program that calculates the number of trees of each species.");
            programDriver.addClass("maxheightree", MaxHeightTree.class,
                    "A map/reduce program that calculates the height of the tallest tree of each kind.");
            programDriver.addClass("sort", Sort.class,
                    "A map/reduce program that sort the trees height from smallest to largest.");
            programDriver.addClass("oldestree", OldestTree.class,
                    "A map/reduce program that displays the district where the oldest tree is.");
            programDriver.addClass("mostree", MostTree.class,
                    "A map/reduce program that displays the district that contains the most trees.");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
