package test20171109;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Test {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("wc");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> text = sc.textFile("test.txt");
		
		JavaRDD<String> words=text.flatMap(new FlatMapFunction<String, String>() {

			public Iterable<String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				return  Arrays.asList(line.split(""));
			}
		});
		JavaPairRDD<String, Integer>  paris = words.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String word) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String,Integer>(word,1);
			}
		});
		
		JavaPairRDD<String, Integer> results = paris.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer value1, Integer value2) throws Exception {
				// TODO Auto-generated method stub
				return value1+value2;
			}
		});
		
		JavaPairRDD<Integer, String>  temp = results.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer,String>(tuple._2,tuple._1);
			}
			
		});
		 JavaPairRDD<String, Integer> sorted = temp.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>,String,Integer>() {

			public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String,Integer>(tuple._2,tuple._1);
			}
		});
		 
		 sorted.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println("word:  "+tuple._1+"  ,  count:   "+tuple._2);
				
			}
		});
		sc.close();
		
		
	}
}
