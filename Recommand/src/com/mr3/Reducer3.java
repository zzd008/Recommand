package com.mr3;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer<Text,Text,Text,Text>{
	private Map<String,String> mapB=new LinkedHashMap<String,String>();
	private boolean flag=true;
	public void reduce(Text inKey,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		Map<String,String> mapA=new LinkedHashMap<String,String>();
		for(Text t:values){
			String str=t.toString();//用new String(t.getBytes())有问题，妈的
			if(str.startsWith("A")){//同现矩阵
				String[] split = str.split(":");
				mapA.put(split[1],split[2]);
			}else{//评分矩阵
				if(flag){//用户的评分只赋值一次即可
					String[]split=str.split("\t");
					String userId=split[0].split(":")[1];
					String score="";
					for(int i=1;i<split.length;i++){
						score+=split[i]+"\t";
					}
					mapB.put(userId, score);
				}
			}
		}
		flag=false;

		//遍历，计算结果
		for(Entry<String, String> e:mapB.entrySet()){
			int flag1=0;
			String key=e.getKey();//输出key
			int value=0;//累加结果
			String str=e.getValue();//该用户的所有评分
			String[] split = str.split("\t");//每一个评分
			for(String i:split){
				String[] split2 = i.split(":");//继续切割
				String item=split2[0];//物品
				if(item.equals(inKey.toString())){//该用户已经购买该物品
					flag1=1;
				}
				int score=Integer.parseInt(split2[1]);//评分
				//匹配同现矩阵，计算求和
				if(mapA.get(item)!=null){
					int count=Integer.parseInt(mapA.get(item));
					value+=(count*score);
				}
			}
			
			if(flag1!=1){
				context.write(new Text(key), new Text(inKey+"\t"+value));
//				System.out.println("reduce3输出："+key+" "+inKey+"\t"+value);
			}
			else{
				context.write(new Text(key), new Text(inKey+"\t"+value+"\t"+"已购买"));
//				System.out.println("reduce3输出："+key+" "+inKey+"\t"+value+"\t"+"已购买");
			}
		}

		
		/*
		for(Entry<String, String> e:mapA.entrySet()){
			System.out.println(e.getKey()+" "+e.getValue());
		}
		for(Entry<String, String> e:mapB.entrySet()){
			System.out.println(e.getKey()+" "+e.getValue());
		}*/
		
	}
}
