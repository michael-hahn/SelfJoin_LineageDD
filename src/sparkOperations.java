import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by Michael on 11/21/15.
 */
public class sparkOperations implements Serializable {
    JavaPairRDD<String, String> sparkWorks(JavaRDD<String> text) {
        JavaRDD<String> filteredLines = text.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String line = new String();
                String kMinusOne = new String();
                String kthItem = new String();
                int index;

                index = s.lastIndexOf(",");
                if (index == -1) {
                    return false;
                } else return true;
            }
        });

        JavaPairRDD<String, String> maps = filteredLines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String line = new String();
                String kMinusOne = new String();
                String kthItem = new String();
                int index;

                index = s.lastIndexOf(",");
                if (index == -1) {
                    System.out.println("MapToPair: Input File in Wrong Format When Processing " + s);
                }
                kMinusOne = s.substring(0, index);
                kthItem = s.substring(index + 1);

                Tuple2<String, String> elem = new Tuple2<String, String>(kMinusOne, kthItem);
                return elem;
            }
        });

        long count = maps.count();
        //System.out.println(count);

        JavaPairRDD<String, Iterable<String>> groupedMap = maps.groupByKey();

        count = groupedMap.count();
        //System.out.println(count);

        JavaPairRDD<String, Iterable<String>> distinctValueMap = groupedMap.mapValues(new Function<Iterable<String>, Iterable<String>>() {
            @Override
            public Iterable<String> call(Iterable<String> strings) throws Exception {
                List<String> kthItemList = new ArrayList<String>();
                for(String s: strings) {
                    if (!kthItemList.contains(s)) {
                        kthItemList.add(s);
                    }
                }
                Collections.sort(kthItemList);
                return kthItemList;
            }
        });

        count = distinctValueMap.count();

        JavaPairRDD<String, String> result = distinctValueMap.flatMapValues(new Function<Iterable<String>, Iterable<String>>() {
            @Override
            public Iterable<String> call(Iterable<String> strings) throws Exception {
                List<String> output = new ArrayList<String>();
                List<String> kthItemList = (List) strings;
                String outVal = new String("");
                for (int i = 0; i < kthItemList.size() - 1; i++) {
                    for (int j = i + 1; j < kthItemList.size(); j++) {
                        outVal = kthItemList.get(i) + "," + kthItemList.get(j);
                        output.add(outVal);
                    }
                }
                return output;
            }
        });
        return result;
    }
}
