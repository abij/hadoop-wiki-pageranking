package com.xebia.sandbox.hadoop.job1.xmlhakker;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WikiLinksReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String pagerank = "1.0\t";

        boolean first = true;

        for (Text value : values) {
            if(!first) pagerank += ",";

            pagerank += value.toString();
            first = false;
        }

        context.write(key, new Text(pagerank));
    }
}
