
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Spark {

    public static void main(String[] args) {
        System.out.println("Hello Spark!");

        // Disable logging loggers;
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // Create a Java Spark configuration;
        SparkConf sparkConf = new SparkConf();

        // set Spark configuration name;
        sparkConf.setAppName("Hello Spark - WordCount");

        // set Spark cores usages count [*]-all, [2]-two cores in the local system;
        sparkConf.setMaster("local[*]");

        //create Spark context using spark configuration defined above
        SparkContext sparkContext = new SparkContext(sparkConf);

        //make RDD object using spark context; load input data from the text file; 1 is count of files for results
        JavaRDD<String> lines = sparkContext.textFile("input.txt",1).toJavaRDD();

        // split lines into acceptable strings; 1 relevant string per 1 map
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                Pattern p = Pattern.compile("\\{.*?\\}");   // the pattern to search for
                Matcher m = p.matcher(s);
                ArrayList<String> ar = new ArrayList<>();
                while (m.find()) {
                    ar.add(m.group());
                }
                return ar.iterator();
            }
        });

        // Transform into word and count.
        JavaPairRDD<String,Integer> mapToPairTransformation = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                Pattern p1 = Pattern.compile("city:\\s*?\"\\w*\"");   // the pattern to search for
                Matcher m1 = p1.matcher(s);
                m1.find();
                Pattern p2 = Pattern.compile("name:\\s*?\\w*,");   // the pattern to search for
                Matcher m2 = p2.matcher(s);
                m2.find();
                return new Tuple2<>(m1.group()+m2.group(), 1);
            }
        });

        // make RDD using reduce for constructed mapped items in the RDD
        JavaPairRDD<String, Integer> reduceByKeyAction = mapToPairTransformation.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return 1;
            }
        });

        //
        JavaRDD<String> transformValuesIntoLevel2 = reduceByKeyAction.map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> s) throws Exception {
                Pattern p = Pattern.compile("name:\\s*?\\w*,");   // the pattern to search for
                Matcher m = p.matcher(s._1());
                m.find();
                return m.group();
            }
        });

        // Transform into word and count.
        JavaPairRDD<String,Integer> mapToPairTransformation2 = transformValuesIntoLevel2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                Pattern p2 = Pattern.compile("name:\\s*?\\w*,");   // the pattern to search for
                Matcher m2 = p2.matcher(s);
                m2.find();
                return new Tuple2<>(m2.group(), 1);
            }
        });

        // make RDD using reduce for constructed mapped items in the RDD
        JavaPairRDD<String, Integer> reduceByKeyAction2 = mapToPairTransformation2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        // make RDD from tuple "phrase" + "count of detection" for result
        JavaRDD<String> transformValuesIntoReadableResults = reduceByKeyAction2.map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> v1) throws Exception {
                return "[" + v1._1()+"] has visited "+v1._2()+" city(ies)";
            }
        });

        // Save the word counts back out to a text file
        transformValuesIntoReadableResults.saveAsTextFile("result"+new SimpleDateFormat("yyyyMMddHHmm").format(new Date()));
    }
}
