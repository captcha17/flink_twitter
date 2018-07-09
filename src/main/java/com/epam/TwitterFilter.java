package com.epam;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Dzmitry_Rakushau on 6/19/2018.
 */
public class TwitterFilter implements TwitterSource.EndpointInitializer, Serializable {
    public static final List<String> TAG_ARRAY = new ArrayList<String>(Arrays.asList("Ronaldo", "Discovery", "HTTP"));
    
    @Override
    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(TAG_ARRAY);
        return endpoint;
    }
}
