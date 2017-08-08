package com.mr2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text,IntWritable,Text,IntWritable>{
	public void reduce(Text inKey,Iterable<IntWritable> inValues,Context context) throws IOException, InterruptedException{
		int sum=0;
		for(IntWritable iw:inValues){
			sum+=1;
		}
		context.write(new Text(inKey), new IntWritable(sum));
//		System.out.println("reduce2输出："+inKey+" "+sum);
	}
}
