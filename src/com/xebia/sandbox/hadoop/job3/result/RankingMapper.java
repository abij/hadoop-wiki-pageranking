package com.xebia.sandbox.hadoop.job3.result;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RankingMapper extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable, Text> {
    
    public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output, Reporter arg3) throws IOException {
        String[] pageAndRank = getPageAndRank(key, value);
        
        float parseFloat = Float.parseFloat(pageAndRank[1]);
        
        Text page = new Text(pageAndRank[0]);
        FloatWritable rank = new FloatWritable(parseFloat);
        
        output.collect(rank, page);
    }
    
    private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
        String[] pageAndRank = new String[2];
        int tabPageIndex = value.find("\t");
        int tabRankIndex = value.find("\t", tabPageIndex + 1);
        
        // no tab after rank (when there are no links)
        int end;
        if (tabRankIndex == -1) {
            end = value.getLength() - (tabPageIndex + 1);
        } else {
            end = tabRankIndex - (tabPageIndex + 1);
        }
        
        pageAndRank[0] = Text.decode(value.getBytes(), 0, tabPageIndex);
        pageAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1, end);
        
        return pageAndRank;
    }
    
}
