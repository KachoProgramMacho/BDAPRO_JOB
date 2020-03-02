/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bdapro;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

public class VideoStreamingJob {

    public static void main(String... args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cloud-30:9092");
        properties.setProperty("zookeeper.connect", "cloud-12:2181");
        properties.setProperty("group.id", "demoGROUPID");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011<>("demo", new SimpleStringSchema(), properties).setStartFromEarliest());


//        DataStream<String> videoEvents = env.socketTextStream("localhost", 7777, "\n");

//        env.readTextFile("PATH_TO_FILE");

        stream.map(new InitialMapStringToEventTuple())
//                .keyBy(1)//video ID
//                .countWindow(1000000)
                .countWindowAll(100000000)
                .aggregate(new AdvertisementCategoryAggregator())
                .flatMap(new CategoryTokenizer())
//                .keyBy(1)
//                .countWindow(80)
                .countWindowAll(1)
                .reduce(new CategoryReducer())
                .map(new MapFunction<Tuple4<Long, String, Float, Integer>, Tuple4<Long, String, Float, Integer>>() {
                    @Override
                    public Tuple4<Long, String, Float, Integer> map(Tuple4<Long, String, Float, Integer> a) throws Exception {
                        a.setField(new Date().getTime() - a.f0, 0);
                        return a;
                    }
                })
                .writeAsText("file:////share/hadoop/rangelov/BigDataAnalysisProject/flink-1.6.0/ABOUTBOYKO_FOREVER.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

//        stream.writeAsText("SLIPKNOT.csv");
        env.execute();
    }



    static class InitialMapStringToEventTuple implements MapFunction<String, Tuple5<Long, String, String, String, Float>> {

        @Override
        public Tuple5<Long, String, String, String, Float> map(String s) throws Exception {
            String[] eventAttributes = s.split(",");
            Float watchedPercentage = -1f;

            //Check if watched or clicked event
            if (eventAttributes.length > 4) {
                watchedPercentage = Float.parseFloat(eventAttributes[4]);
            }
            Double d = Double.parseDouble(eventAttributes[0].trim());
            Long l = d.longValue();
            return new Tuple5<Long, String, String, String, Float>(l, eventAttributes[1], eventAttributes[2], eventAttributes[3], watchedPercentage);
        }
    }

    static class AdvertisementCategoryAggregator implements AggregateFunction<Tuple5<Long, String, String, String, Float>, CustomAcc, CustomAcc> {
         @Override
        public CustomAcc createAccumulator() {
            return new CustomAcc();
        }

        @Override
        public CustomAcc add(Tuple5<Long, String, String, String, Float> event, CustomAcc acc) {
            boolean isClicked = event.f4.equals(-1f);
            Integer eventValue = Math.round(event.f4 * 5);
            if (isClicked) {
                eventValue = 10;
            }

            acc.aggregateEvent(event.f2,eventValue);

            // Store the latest timestamp in the accumlator tuple`
            if(acc.timestamp < event.f0){
                acc.timestamp = event.f0;
            }
            return acc;
        }

        @Override
        public CustomAcc getResult(CustomAcc acc) {
            return acc;
        }

        @Override
        public CustomAcc merge(CustomAcc acc1, CustomAcc acc2) {
            acc1.mergeWithAcc(acc2);
            return acc1;
        }
    }

    public static class CategoryTokenizer implements FlatMapFunction<CustomAcc,Tuple4<Long,String, Float, Integer>>{

        @Override
        public void flatMap(CustomAcc acc, Collector<Tuple4<Long, String, Float, Integer>> collector) throws Exception {
            for(String category: acc.categories){
                int categoryIndex = acc.categories.indexOf(category);
                Tuple4<Long,String,Float, Integer> outputTuple = new Tuple4<Long,String, Float, Integer>(acc.timestamp,category, acc.sums.get(categoryIndex),acc.counts.get(categoryIndex));
                collector.collect(outputTuple);
            }
        }
    }

    public static class CategoryReducer implements ReduceFunction<Tuple4<Long,String, Float, Integer>>{

        @Override
        public Tuple4<Long,String, Float, Integer> reduce(Tuple4<Long,String, Float, Integer> a, Tuple4<Long,String, Float, Integer> b) throws Exception {
            long largestTimestamp = a.f0 >= b.f0 ? a.f0 : b.f0;
            return new Tuple4(largestTimestamp,a.f1,a.f2+b.f2,a.f3+b.f3);
        }
    }
}