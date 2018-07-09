package com.epam;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Dzmitry_Rakushau on 6/19/2018.
 */
public class TweetFlatMapper implements FlatMapFunction<String, Tuple2<String, String>> {
    private static final String LINK = "https://twitter.com/Dmitry61285261/status/";
    
    @Override
    public void flatMap(String tweet, Collector<Tuple2<String, String>> out) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String tweetString;

        Pattern p = Pattern.compile("#\\w+");
        JsonNode jsonNode;
        try {
            jsonNode = mapper.readValue(tweet, JsonNode.class);
            tweetString = jsonNode.get("text").textValue();

        if (tweetString != null) {
            Matcher matcher = p.matcher(tweetString);

            while (matcher.find()) {
                String id = jsonNode.get("id").asText();
                String cleanedHashtag = matcher.group(0).trim();
                out.collect(new Tuple2<>(cleanedHashtag, LINK + id));
            }
        }
        
        } catch (Exception e) {
            // That's ok
        }
    }
}
