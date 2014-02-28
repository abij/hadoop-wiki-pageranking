package com.xebia.sandbox.hadoop.job3.result;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

public class RankingMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] pageAndRank = getPageAndRank(key, value);
        
        float parseFloat = Float.parseFloat(pageAndRank[1]);
        
        Text page = new Text(pageAndRank[0]);
        FloatWritable rank = new FloatWritable(parseFloat);

        context.write(rank, page);
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
