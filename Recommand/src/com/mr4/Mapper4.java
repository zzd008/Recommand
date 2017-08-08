package com.mr4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper4 extends Mapper<LongWritable,Text,IntPair,Text>{
	public void map(LongWritable inKey,Text inValue,Context context) throws IOException, InterruptedException{
		IntPair key=new IntPair();
		String value="";
		StringTokenizer st=new StringTokenizer(inValue.toString());
		int first=Integer.parseInt(st.nextToken());
		value+=st.nextToken()+"\t";
		int second=Integer.parseInt(st.nextToken());
		key.set(first, second);
		while(st.hasMoreTokens()){
			value+=st.nextToken();
		}
		context.write(key, new Text(value));
//		System.out.println("map4输出："+"<"+key.getFirst()+","+key.getSecond()+">"+"\t"+value);
	}
	
}
