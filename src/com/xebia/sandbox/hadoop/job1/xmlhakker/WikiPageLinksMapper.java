package com.xebia.sandbox.hadoop.job1.xmlhakker;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class WikiPageLinksMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // Returns  String[0] = <title>[TITLE]</title>
        //          String[1] = <text>[CONTENT]</text>
        // !! without the <tags>.
        String[] titleAndText = parseTitleAndText(value);
        
        String pageString = titleAndText[0];
        if(notValidPage(pageString))
            return;
        
        Text page = new Text(pageString.replace(' ', '_'));

        Matcher matcher = wikiLinksPattern.matcher(titleAndText[1]);
        
        //Loop through the matched links in [CONTENT]
        while (matcher.find()) {
            String otherPage = matcher.group();
            //Filter only wiki pages.
            //- some have [[realPage|linkName]], some single [realPage]
            //- some link to files or external pages.
            //- some link to paragraphs into other pages.
            otherPage = getWikiPageFromLink(otherPage);
            if(otherPage == null || otherPage.isEmpty()) 
                continue;
            
            // add valid otherPages to the map.
            context.write(page, new Text(otherPage));
        }
    }
    
    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String getWikiPageFromLink(String aLink){
        if(isNotWikiLink(aLink)) return null;
        
        int start = aLink.startsWith("[[") ? 2 : 1;
        int endLink = aLink.indexOf("]");

        int pipePosition = aLink.indexOf("|");
        if(pipePosition > 0){
            endLink = pipePosition;
        }
        
        int part = aLink.indexOf("#");
        if(part > 0){
            endLink = part;
        }
        
        aLink =  aLink.substring(start, endLink);
        aLink = aLink.replaceAll("\\s", "_");
        aLink = aLink.replaceAll(",", "");
        aLink = sweetify(aLink);
        
        return aLink;
    }
    
    private String sweetify(String aLinkText) {
        if(aLinkText.contains("&amp;"))
            return aLinkText.replace("&amp;", "&");

        return aLinkText;
    }

    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.
        
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        
        return titleAndText;
    }

    private boolean isNotWikiLink(String aLink) {
        int start = 1;
        if(aLink.startsWith("[[")){
            start = 2;
        }
        
        if( aLink.length() < start+2 || aLink.length() > 100) return true;
        char firstChar = aLink.charAt(start);
        
        if( firstChar == '#') return true;
        if( firstChar == ',') return true;
        if( firstChar == '.') return true;
        if( firstChar == '&') return true;
        if( firstChar == '\'') return true;
        if( firstChar == '-') return true;
        if( firstChar == '{') return true;
        
        if( aLink.contains(":")) return true; // Matches: external links and translations links
        if( aLink.contains(",")) return true; // Matches: external links and translations links
        if( aLink.contains("&")) return true;
        
        return false;
    }
}
