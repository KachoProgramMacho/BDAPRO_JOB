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
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VideoStreamingJob {

	public static void main(String... args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> videoEvents = env.socketTextStream("localhost", 7777, "\n");

		//env.readTextFile("PATH_TO_FILE");

		videoEvents.map(new InitialMapStringToEventTuple())
				.keyBy(1)//video ID
				.countWindow(100)
				.aggregate(new AdvertisementCategoryAggregator())
				.writeAsText("ABOUTBOYKO.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


		env.execute();
	}


	static class InitialMapStringToEventTuple implements MapFunction<String, Tuple5<Long, String, String, String, Float>>{

		@Override
		public Tuple5<Long, String, String, String, Float> map(String s) throws Exception {
			String[] eventAttributes = s.split(",");
			Float watchedPercentage = -1f;
			if(eventAttributes.length>4){
				watchedPercentage = Float.parseFloat(eventAttributes[4]);
			}
			Double d = Double.parseDouble(eventAttributes[0].trim());
			Long l = d.longValue();
			return new Tuple5<Long, String, String, String, Float>(l, eventAttributes[1] ,eventAttributes[2] ,eventAttributes[3], watchedPercentage);
		}
	}

	static class AdvertisementCategoryAggregator implements AggregateFunction<Tuple5<Long, String, String, String, Float>, Tuple4<Integer, Integer, Integer, Integer>, Tuple2<Integer, Integer>> {

		public static final String ANSI_RESET = "\u001B[0m";
		public static final String ANSI_RED = "\u001B[31m";

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> createAccumulator() {
			return new Tuple4<Integer, Integer, Integer, Integer>(0, 0, 0, 0);
		}

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> add(Tuple5<Long, String, String, String, Float> event, Tuple4<Integer, Integer, Integer, Integer> acc) {
			boolean isClicked = event.f4.equals(-1f);
			Integer eventValue = Math.round(event.f4*5);
			if(isClicked){
				eventValue = 10;
			}

			switch (event.f2){
				case "FOOD":
					acc.setField(acc.f0 + eventValue,0);
					acc.setField(acc.f1 + 1,1);
					break;
				case "ELECTRONICS":
					acc.setField(acc.f2 + eventValue,2);
					acc.setField(acc.f3 + 1,3);
					break;
				default:
					System.out.println(ANSI_RED + "Wrong advertisement category: " + event.f2 + ANSI_RESET);
					break;
			}
			return acc;
		}

		@Override
		public Tuple2<Integer, Integer> getResult(Tuple4<Integer, Integer, Integer, Integer> acc) {
			Integer category1 = 0;
			Integer category2 = 0;
			if(acc.f1 != 0){
				category1 = acc.f0 / acc.f1;
			}
			if(acc.f3 != 0){
				category2 =  acc.f2 / acc.f3;
			}
			return new Tuple2<>(category1, category2);
		}

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> merge(Tuple4<Integer, Integer, Integer, Integer> acc1, Tuple4<Integer, Integer, Integer, Integer> acc2) {
			return new Tuple4<Integer, Integer, Integer, Integer>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1, acc1.f2 + acc2.f2, acc1.f3 + acc2.f3);
		}
	}
}