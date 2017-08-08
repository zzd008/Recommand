package com.mr4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer4 extends Reducer<IntPair,Text,Text,Text>{
	public void reduce(IntPair inKey,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		for(Text t:values){
			String[] split = t.toString().split("\t");
			if(split.length==2){
				context.write(new Text(String.valueOf(inKey.getFirst())), new Text(split[0]+"\t"+inKey.getSecond()+"\t"+split[1]));
//				System.out.println("reduce4输出："+String.valueOf(inKey.getFirst())+" "+split[0]+"\t"+inKey.getSecond()+"\t"+split[1]);
			}else{
				context.write(new Text(String.valueOf(inKey.getFirst())), new Text(split[0]+"\t"+inKey.getSecond()));
//				System.out.println("reduce4输出："+String.valueOf(inKey.getFirst())+" "+split[0]+"\t"+inKey.getSecond());
			}
		}
	}
}
