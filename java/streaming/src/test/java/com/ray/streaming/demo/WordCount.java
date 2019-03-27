package com.ray.streaming.demo;

import com.ray.streaming.api.context.StreamingContext;
import com.ray.streaming.api.function.impl.FlatMapFunction;
import com.ray.streaming.api.function.impl.KeyFunction;
import com.ray.streaming.api.function.impl.ReduceFunction;
import com.ray.streaming.api.function.impl.SinkFunction;
import com.ray.streaming.api.stream.StreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class WordCount {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) {
        StreamingContext streamingContext = StreamingContext.buildContext();
        List<String> value = new ArrayList<>();
        value.add("hello world eagle");
        StreamSource<String> streamSource = StreamSource.buildSource(streamingContext, value);
        streamSource
                .flatMap((FlatMapFunction<String, KeyWord>) (value1, collector) -> {
                    String[] records = value1.split(" ");
                    for (String record : records) {
                        collector.collect(new KeyWord(record, 1));
                    }
                })
                .keyBy((KeyFunction<KeyWord, String>) value12 -> value12.getKey())
                .reduce((ReduceFunction<KeyWord>) (oldValue, newValue) ->
                        new KeyWord(oldValue.getKey(), oldValue.getCount() + newValue.getCount()))
                .sink((SinkFunction<KeyWord>) value13 -> LOGGER.info("result is {}", value13));

        streamingContext.execute();
    }

    static class KeyWord implements Serializable {
        private String key;
        private Integer count;

        public KeyWord(String key, Integer count) {
            this.key = key;
            this.count = count;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "key:" + key + ",value:" + count;
        }
    }

}
