package hydrasoft;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;


import java.util.Arrays;

public class BatchJob {
    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        String inputFile = params.get("input", "poker-hand-testing.csv");
        String outputFile = params.get("output", "outputPokerAceAnalysis.csv");

        DataSet<String> pokerHands = env.readTextFile(inputFile);
        DataSet<Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> tokenizedPokerHands =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                pokerHands.flatMap(new Tokenizer());

        DataSet<Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> atLeastOneAceSet = tokenizedPokerHands.filter(new FilterFunction<Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> hand) throws Exception {
                        // Return true if there's at least one Ace in the hand
                        return hand.f1.equals(1) || hand.f3.equals(1) || hand.f5.equals(1) || hand.f7.equals(1) || hand.f9.equals(1);
                    }
                });
        DataSet<Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> fixedAceSet = atLeastOneAceSet.filter(new FilterFunction<Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> aceHand) throws Exception {
                int numberOfAces = (aceHand.f1.equals(1) ? 1 : 0) + (aceHand.f3.equals(1) ? 1 : 0)+ (aceHand.f5.equals(1) ? 1 : 0)+ (aceHand.f7.equals(1) ? 1 : 0) + (aceHand.f9.equals(1) ? 1 : 0);
                if(numberOfAces==3){
                    return true;
                }
                return false;
            }
        });
        DataSet<Tuple2<Integer,Integer>> crossResult = fixedAceSet.cross(tokenizedPokerHands).with(new PokerHandCrossComputer());
        DataSet<Tuple2<Integer, Integer>> aggregatedResults = crossResult.reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) throws Exception {
                return new Tuple2<Integer, Integer>(a.f0+b.f0, a.f1+b.f1);
            }
        });


        String acePercentage = Long.toString(atLeastOneAceSet.count()/pokerHands.count());
        aggregatedResults.writeAsCsv(outputFile, "\n", ",", FileSystem.WriteMode.OVERWRITE);



        // execute program
        env.execute("Flink Batch Java API Skeleton");
    }
        public static class Tokenizer implements FlatMapFunction<String, Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {

            @Override
            public void flatMap(String value, Collector<Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) {
                // normalize and split the line
                String[] tokens = value.toLowerCase().split("\\W+");

                int[] tokenItems = Arrays.asList(tokens).stream().mapToInt(Integer::parseInt).toArray();

                out.collect(new Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(tokenItems[0],
                        tokenItems[1], tokenItems[2], tokenItems[3], tokenItems[4], tokenItems[5], tokenItems[6], tokenItems[7], tokenItems[8], tokenItems[9], tokenItems[10]));

            }
        }

        public static class PokerHandCrossComputer implements CrossFunction<Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple2<Integer, Integer>> {


            @Override
            public Tuple2<Integer, Integer> cross(Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tupleAcePokerHands, Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tupleAllPokerHands) throws Exception {
                int resultComparison = HandComparator.compareHands(tupleAcePokerHands,tupleAllPokerHands);
                return new Tuple2<Integer, Integer>(1,resultComparison);
            }
        }


}


