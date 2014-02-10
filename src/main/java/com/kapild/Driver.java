package com.kapild;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.kapild.TextPair;
import com.kapild.TextPairComparator;
import com.kapild.TextPairPartitioner;
import com.kapild.WordCountMapper;

public class Driver extends Configured implements Tool {
    static Logger logger = Logger.getLogger(Driver.class);

    @Override
    public int run(String[] args) throws Exception {

        String inputDir = args[0];
        String outputDir = args[1];

        Configuration conf = getConf();
        Properties props = new Properties();

        try {
            Job filterFactualLocationJob = runJob(conf, inputDir, outputDir);
            logger.info("info");
            filterFactualLocationJob.waitForCompletion(true);
            logger.info("info done.");

        } catch (Exception e) {
            logger.error("Errorjob", e.fillInStackTrace());
            return -1;
        } finally {
            logger.info("info done.");
        }

        return 0;
    }

    private Job runJob(Configuration conf, String inputDir, String outputDir)
            throws IOException, InterruptedException, ClassNotFoundException,
            ParseException {

        Job runJob = new Job(conf);
        runJob.setJobName(String.format(" Job: %s => %s", inputDir, outputDir));
        runJob.setJarByClass(getClass());

        MultipleInputs.addInputPath(runJob, new Path(inputDir),
                TextInputFormat.class, WordCountMapper.class);

        runJob.setMapOutputKeyClass(Text.class);
        runJob.setMapOutputValueClass(Text.class);
        runJob.setMapOutputKeyClass(TextPair.class);
        runJob.setMapOutputValueClass(TextPair.class);
        runJob.setOutputKeyClass(Text.class);
        runJob.setOutputValueClass(Text.class);

        runJob.setPartitionerClass(TextPairPartitioner.class);
        runJob.setGroupingComparatorClass(TextPairComparator.class);

        FileOutputFormat.setOutputPath(runJob, new Path(outputDir));
        runJob.setNumReduceTasks(10);
        runJob.waitForCompletion(true);

        return runJob;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(res);
    }
}