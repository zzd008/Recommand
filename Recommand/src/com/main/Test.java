package com.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mr1.Mapper1;
import com.mr1.Reducer1;
import com.mr2.Mapper2;
import com.mr2.Reducer2;
import com.mr3.Mapper3;
import com.mr3.Reducer3;
import com.mr4.IntPair;
import com.mr4.Mapper4;
import com.mr4.Reducer4;

public class Test {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker", "localhost:9001");//设置mapreduce运行环境
		
		long start = System.currentTimeMillis();//开始运行时间
		/**
		 * 运行MR１
		 * 构建用户评分矩阵
		 */
//		String []arg=new String[]{"/Recommand/mr1/mr1_in/mr1_in","/Recommand/mr1/mr1_out"};
		String []arg=new String[]{"/Recommand/mr1/mr1_in/u.data","/Recommand/mr1/mr1_out"};
		String []otherArgs=new GenericOptionsParser(conf, arg).getRemainingArgs();//文件校验
		if(otherArgs.length!=2){
			System.err.print("mr1的输入或输出文件有误！");
		}
		FileSystem fs=FileSystem.get(conf);//如果输出文件存在，就删除
		if(fs.exists(new Path(arg[1]))){
			fs.delete(new Path(arg[1]),true);
		}
		
		Job job=Job.getInstance(conf,"jobd1");//设置job的运行环境及名称
		job.setJarByClass(Test.class);//设置job要打包处理的类
		job.setMapperClass(Mapper1.class);//设置map处理类
		job.setReducerClass(Reducer1.class);//设置reduce处理类
		job.setOutputKeyClass(IntWritable.class);//设置输出key类型
		job.setOutputValueClass(Text.class);//设置输出value类型
		job.setInputFormatClass(TextInputFormat.class);//设置输入格式类，默认是这个
		job.setOutputFormatClass(TextOutputFormat.class);//设置输出格式类，默认是这个
		//要将core、hdfs-site.xml、log4j文件放到项目下 或者是把输入/出文件的路径写为hdfs://localhost:9000/Recommand/in 不然会被当成本地文件系统的路径
		FileInputFormat.addInputPath(job, new Path(arg[0]));//设置输入文件路径 
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));//设置输出文件路径
		job.waitForCompletion(true);//提交job
		
		/**
		 * 运行MR2
		 * 构建同现矩阵：两两出现的次数
		 */
		String []arg2=new String[]{"/Recommand/mr1/mr1_out","/Recommand/mr2/mr2_out"};
		String []otherArgs2=new GenericOptionsParser(conf, arg2).getRemainingArgs();//文件校验
		if(otherArgs2.length!=2){
			System.err.print("mr2的输入或输出文件有误！"); 
		}
		FileSystem fs2=FileSystem.get(conf);//如果输出文件存在，就删除
		if(fs2.exists(new Path(arg2[1]))){
			fs2.delete(new Path(arg2[1]),true);
		}
		
		Job job2=Job.getInstance(conf,"job2");
		job2.setJarByClass(Test.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(arg2[0]));
		FileOutputFormat.setOutputPath(job2, new Path(arg2[1]));
		job2.waitForCompletion(true);
		
		/**
		 * 运行MR3
		 *同现矩阵*用户评分矩阵
		 *用户i对物品j的喜欢程度=同现矩阵第i行上各列值依次乘以用户i对对应物品的评分，最后求和
		 *对所有用户都计算出推荐结果
		 */
		String []arg3=new String[]{"/Recommand/mr1/mr1_out","/Recommand/mr2/mr2_out","/Recommand/mr3/mr3_out"};
		String []otherArgs3=new GenericOptionsParser(conf, arg3).getRemainingArgs();//文件校验
		if(otherArgs3.length!=3){
			System.err.print("mr3的输入或输出文件有误！"); 
		}
		FileSystem fs3=FileSystem.get(conf);//如果输出文件存在，就删除
		if(fs3.exists(new Path(arg3[2]))){
			fs3.delete(new Path(arg3[2]),true);
		}
		
		Job job3=Job.getInstance(conf,"job3");
		job3.setJarByClass(Test.class);
		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer3.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job3, new Path(arg3[0]));//添加两个输入文件 addInputPaths()方法也可以
		FileInputFormat.addInputPath(job3, new Path(arg3[1]));
		FileOutputFormat.setOutputPath(job3, new Path(arg3[2]));
		job3.waitForCompletion(true);
		
		/**
		 * 运行MR4
		 * 对推荐结果排序
		 */
		
		String []arg4=new String[]{"/Recommand/mr3/mr3_out","/Recommand/mr4/mr4_out"};
		String []otherArgs4=new GenericOptionsParser(conf, arg4).getRemainingArgs();//文件校验
		if(otherArgs4.length!=2){
			System.err.print("mr4的输入或输出文件有误！"); 
		}
		FileSystem fs4=FileSystem.get(conf);//如果输出文件存在，就删除
		if(fs4.exists(new Path(arg4[1]))){
			fs4.delete(new Path(arg4[1]),true);
		}
		
		Job job4=Job.getInstance(conf,"job4");
		job4.setJarByClass(Test.class);
		//job4.setGroupingComparatorClass(GroupingComparator.class); 若未指定则默认使用key的compareTo()方法排序
		job4.setMapperClass(Mapper4.class);
		job4.setReducerClass(Reducer4.class);
		job4.setMapOutputKeyClass(IntPair.class);//设置map输出key
		job4.setMapOutputValueClass(Text.class);//设置map输出value
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job4, new Path(arg4[0]));
		FileOutputFormat.setOutputPath(job4, new Path(arg4[1]));
		job4.waitForCompletion(true);
		
		long end = System.currentTimeMillis();//结束时的时间
		System.out.println("共用时"+(end-start)+"毫秒!");
	}
}
