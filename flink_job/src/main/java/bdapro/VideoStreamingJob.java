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

import org.apache.flink.api.common.functions.*;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

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
                .keyBy(1)//video ID
                .countWindow(1000000)
                .process(new AvgProcessWindowFunction())
                .filter(new FilterFunction<Tuple5<Long, String, String, Float, Integer>>() {
                    @Override
                    public boolean filter(Tuple5<Long, String, String, Float, Integer> a) throws Exception {
                        return (a.f3/a.f4) < 0.05;
                    }
                })
                .keyBy(2)
                .countWindow(10)
                .reduce(new CategoryReducer())
                .map(new MapFunction<Tuple5<Long, String, String, Float, Integer>, Tuple3<Long, String, Float>>() {
                    @Override
                    public Tuple3<Long, String, Float> map(Tuple5<Long, String, String, Float, Integer> a) throws Exception {
                        return new Tuple3<>(a.f0, a.f2, a.f3/a.f4);
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

    private static class AvgProcessWindowFunction extends ProcessWindowFunction<Tuple5<Long, String, String, String, Float>, Tuple5<Long, String, String, Float, Integer>, Tuple, GlobalWindow> {


        @Override
        public void process(Tuple key, Context context, Iterable<Tuple5<Long, String, String, String, Float>> inputs, Collector<Tuple5<Long, String, String, Float, Integer>> collector) throws Exception {
            int count = 0;
            float sum = 0;
            long maxTimeStamp = 0;
            String category = "";
            String adID = "";
            for (Tuple5<Long, String, String, String, Float> in: inputs) {
                category = in.f2;
                adID = in.f1;
                boolean isClicked = in.f4.equals(-1f);
                count++;
                if(isClicked){
                    sum += 10;
                }else {
                    sum += in.f4;
                }

                if(maxTimeStamp<in.f0){
                    maxTimeStamp = in.f0;
                }

            }
            collector.collect(new Tuple5<Long, String, String, Float, Integer>(maxTimeStamp, adID, category, sum, count));
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

    public static class CategoryReducer implements ReduceFunction<Tuple5<Long, String, String, Float, Integer>>{

        @Override
        public Tuple5<Long, String, String, Float, Integer> reduce(Tuple5<Long, String, String, Float, Integer> a, Tuple5<Long, String, String, Float, Integer> b) throws Exception {
            long largestTimestamp = a.f0 >= b.f0 ? a.f0 : b.f0;
            return new Tuple5<Long, String, String, Float, Integer>(largestTimestamp, a.f1,a.f2,a.f3+b.f3,a.f4+b.f4);
        }
    }
}