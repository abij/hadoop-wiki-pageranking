package com.xebia.sandbox.hadoop;

import com.xebia.sandbox.hadoop.job1.xmlhakker.WikiLinksReducer;
import com.xebia.sandbox.hadoop.job1.xmlhakker.WikiPageLinksMapper;
import com.xebia.sandbox.hadoop.job1.xmlhakker.XmlInputFormat;
import com.xebia.sandbox.hadoop.job2.calculate.RankCalculateMapper;
import com.xebia.sandbox.hadoop.job2.calculate.RankCalculateReduce;
import com.xebia.sandbox.hadoop.job3.result.RankingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class WikiPageRanking extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new WikiPageRanking(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        boolean isCompleted = runXmlParsing("wiki/in", "wiki/ranking/iter00");
        if (!isCompleted) return 1;

        String lastResultPath = null;

        for (int runs = 0; runs < 5; runs++) {
            String inPath = "wiki/ranking/iter" + nf.format(runs);
            lastResultPath = "wiki/ranking/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }

        isCompleted = runRankOrdering(lastResultPath, "wiki/result");

        if (!isCompleted) return 1;
        return 0;
    }


    public boolean runXmlParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        Job xmlHakker = Job.getInstance(conf, "xmlHakker");
        xmlHakker.setJarByClass(WikiPageRanking.class);

        // Input / Mapper
        FileInputFormat.addInputPath(xmlHakker, new Path(inputPath));
        xmlHakker.setInputFormatClass(XmlInputFormat.class);
        xmlHakker.setMapperClass(WikiPageLinksMapper.class);
        xmlHakker.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(xmlHakker, new Path(outputPath));
        xmlHakker.setOutputFormatClass(TextOutputFormat.class);

        xmlHakker.setOutputKeyClass(Text.class);
        xmlHakker.setOutputValueClass(Text.class);
        xmlHakker.setReducerClass(WikiLinksReducer.class);

        return xmlHakker.waitForCompletion(true);
    }

    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(WikiPageRanking.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(RankCalculateMapper.class);
        rankCalculator.setReducerClass(RankCalculateReduce.class);

        return rankCalculator.waitForCompletion(true);
    }

    private boolean runRankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankOrdering = Job.getInstance(conf, "rankOrdering");
        rankOrdering.setJarByClass(WikiPageRanking.class);

        rankOrdering.setOutputKeyClass(FloatWritable.class);
        rankOrdering.setOutputValueClass(Text.class);

        rankOrdering.setMapperClass(RankingMapper.class);

        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

        rankOrdering.setInputFormatClass(TextInputFormat.class);
        rankOrdering.setOutputFormatClass(TextOutputFormat.class);

        return rankOrdering.waitForCompletion(true);
    }

}
