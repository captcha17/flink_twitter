package com.epam;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Created by Dzmitry_Rakushau on 6/19/2018.
 */
public class MostPopularTags implements AllWindowFunction<Tuple2<String, String>, LinkedHashMap<String, Integer>, TimeWindow> {
    private static final Integer HASHTAG_LIMIT = 10;
    
    @Override
    public void apply(TimeWindow window, Iterable<Tuple2<String, String>> tweets, Collector<LinkedHashMap<String, Integer>> collector) {
        HashMap<String, Integer> hmap = new HashMap<>();

        for (Tuple2<String, String> t: tweets) {
            int count = 1;
            if (hmap.containsKey(t.f0)) {
                count = hmap.get(t.f0);
                count++;
            }
            hmap.put(t.f0, count);
        }

        Comparator<String> comparator = new ValueComparator(hmap);
        TreeMap<String, Integer> sortedMap = new TreeMap<>(comparator);

        sortedMap.putAll(hmap);

        LinkedHashMap<String, Integer> sortedTopN = sortedMap
                .entrySet()
                .stream()
                .limit(HASHTAG_LIMIT)
                .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

        collector.collect(sortedTopN);
    }

    public static class ValueComparator implements Comparator<String> {
        HashMap<String, Integer> map = new HashMap<>();

        ValueComparator(Map<String, Integer> map){
            this.map.putAll(map);
        }

        @Override
        public int compare(String s1, String s2) {
            return map.get(s2).compareTo(map.get(s1));
        }
    }
}
