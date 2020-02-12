package hydrasoft;


import org.apache.flink.api.java.tuple.Tuple11;

public class HandComparator {
    private static HandComparator instance = null;

    private HandComparator(){

    }

    public static HandComparator getInstance(){
        if(instance==null){
            instance = new HandComparator();
        }
        return instance;
    }

    public static int compareHands(Tuple11<Integer, Integer,Integer, Integer,Integer, Integer,Integer, Integer,Integer, Integer, Integer> a, Tuple11<Integer, Integer,Integer, Integer,Integer, Integer,Integer, Integer,Integer, Integer, Integer> b){

        // If a smaller or equal than b => return -1 else return 1
        if(a.f10.compareTo(b.f10)==-1  ){
            return 0;
        }

       return 1;

    }
}
