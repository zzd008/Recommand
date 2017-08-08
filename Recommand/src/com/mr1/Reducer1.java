package com.mr1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer1 extends Reducer<IntWritable,Text,IntWritable,Text>{
	public void reduce(IntWritable inKey,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		int key = inKey.get();
		String value="";
		for(Text t:values){
			String s = t.toString();
			value+=s+"\t";
		}
		context.write(new IntWritable(key), new Text(value));
//		System.out.println("reduce1输出："+key+"\t"+value);
	}
}