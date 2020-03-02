package bdapro;

import java.util.ArrayList;

public class CustomAcc {

    long timestamp;
    ArrayList<String> categories;
    ArrayList<Float> sums;
    ArrayList<Integer>counts;

    public CustomAcc(){
        this.timestamp = 0;
        this.categories = new ArrayList<String>();
        this.sums = new ArrayList<Float>();
        this.counts = new ArrayList<Integer>();
    }

    public void aggregateEvent(String newCategory, float eventScore ){
        int categoryIndex = this.categories.indexOf(newCategory);
        if(categoryIndex!=-1){
            sums.set(categoryIndex,sums.get(categoryIndex)+eventScore);
            counts.set(categoryIndex, counts.get(categoryIndex) + 1);
        }else{
            //If category doesn't exist yet
            categories.add(newCategory);
            sums.add(eventScore);
            counts.add(1);
        }
    }

    public void mergeWithAcc(CustomAcc acc2){

        ArrayList<String> acc2Categories = acc2.categories;
        for(String newCategory:acc2Categories){
            int acc2CategoryIndex = acc2Categories.indexOf(newCategory);
            int myCategoryIndex = this.categories.indexOf(newCategory);

            if(myCategoryIndex!=-1){
                sums.set(myCategoryIndex,sums.get(myCategoryIndex)+acc2.sums.get(acc2CategoryIndex));
                counts.set(myCategoryIndex, counts.get(myCategoryIndex) + acc2.counts.get(acc2CategoryIndex));
            }else{
                //If category doesn't exist yet
                categories.add(newCategory);
                sums.add(acc2.sums.get(acc2CategoryIndex));
                counts.add(acc2.counts.get(acc2CategoryIndex));
            }
        }
    }
@Override
    public String toString(){
        return this.counts.toString() + " " + this.categories.toString();
    }
}
