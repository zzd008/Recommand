package com.mr3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper3 extends Mapper<LongWritable,Text,Text,Text>{
	private static ArrayList<String> list=new ArrayList<String>();
	private static String []items;
	static{//读取文件中共多少种商品
//		String filePath="hdfs://localhost:9000/Recommand/mr1/mr1_in/mr1_in";
		String filePath="hdfs://localhost:9000/Recommand/mr1/mr1_in/u.data";
			try {
				FileSystem fs=FileSystem.get(URI.create("hdfs://localhost:9000"),new Configuration());
				FSDataInputStream read = fs.open(new Path(filePath));
				InputStreamReader in=new InputStreamReader(read);//转换成字符流
				BufferedReader br=new BufferedReader(in);//包装
				String str;
				while((str=br.readLine())!=null&&str.length()!=0){//过滤文件中的空行
					String[] split = str.split("\t");
					if(!(list.contains(split[1]))){
						list.add(split[1]);
					}
				}
				items=new String[list.size()];//存入数组
				int n=0;
				for(String i:list){
					items[n++]=i;
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
	}
	public void map(LongWritable inKey,Text inValue,Context context) throws IOException, InterruptedException{
		String line=inValue.toString();
		StringTokenizer st=new StringTokenizer(line, "\t");
		String flagStr=st.nextToken();
		if(flagStr.contains(":")){//为同现矩阵
			String[] split = flagStr.split(":");
			String key=split[0];
			String value="";
			while(st.hasMoreTokens()){//标识
				value+="A"+":"+split[1]+":"+st.nextToken();
			}
			context.write(new Text(key), new Text(value));
//			System.out.println("map3输出："+key+" "+value);
		}else{//为评分矩阵
			for(String i:items){//遍历，使同现矩阵中的每一行都对应所有用户，并保证他们被同一个reduce处理(另输出的key相同)
				String value="";
				value+="B"+":"+line;
				context.write(new Text(i), new Text(value));
//				System.out.println("map3输出："+i+"\t"+value);
			}
		}
		
		
	}
}
