package martiflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class PokerAceAnalysis {

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final String OUTPUT_PATH = "C:\\cygwin64\\home\\marti\\data\\outputAceAnalysis.txt";
        final String INPUT_PATH = "C:\\cygwin64\\home\\marti\\data\\inputPokerHands.txt";
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(INPUT_PATH);

        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new hydrasoft.PokerCombinationCount.Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        counts.writeAsCsv("C:\\cygwin64\\home\\marti\\data\\output.txt",";"," ", FileSystem.WriteMode.OVERWRITE);
        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataSet<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * http://flink.apache.org/docs/latest/apis/batch/index.html
         *
         * and the examples
         *
         * http://flink.apache.org/docs/latest/apis/batch/examples.html
         *
         */


        // execute program
        env.execute("Flink Batch Java API Skeleton");
    }
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");


            out.collect(new Tuple2<String, Integer>(tokens[tokens.length-1], 1));

        }
    }
}
