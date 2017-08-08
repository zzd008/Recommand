package com.mr1;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Mapper1 extends Mapper<Object,Text,IntWritable,Text>{
	public void map(Object inKey,Text inValue,Context context) throws IOException, InterruptedException{
		int key;//输出key
		String value="";//输出value
		String line=inValue.toString();
		StringTokenizer st=new StringTokenizer(line,"\t");
		while(st.hasMoreTokens()){
			key=Integer.parseInt(st.nextToken());
			value=value+st.nextToken()+":"+st.nextToken();
			context.write(new IntWritable(key),new Text(value));//输出数据默认以\t分割
//			System.out.println("map1输出："+key+"\t"+value);
			break;
		}
	}
}
