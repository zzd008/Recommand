package com.mr2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object,Text,Text,IntWritable>{
	public void map(Object inKey,Text inValue,Context context) throws IOException, InterruptedException{
		String line=inValue.toString();
		StringTokenizer st=new StringTokenizer(line,"\t");
		st.nextToken();//过滤掉用户id
		ArrayList<String> list=new ArrayList<String>();
		while(st.hasMoreTokens()){
			String str = st.nextToken();
			String[] split = str.split(":");
			list.add(split[0]);//存储电影id
		}
		String items[]=new String[list.size()];
		int n=0;
		for(String i:list){
			items[n++]=i;
		}
		for(int i=0;i<items.length;i++){//两两出现的次数
			for(int j=0;j<items.length;j++){
				String value=items[i]+":"+items[j];
				context.write(new Text(value), new IntWritable(1));
//				System.out.println("map2输出："+value+" "+1);
			}
		}
	}
}
