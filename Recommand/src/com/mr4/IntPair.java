package com.mr4;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public  class IntPair implements WritableComparable<IntPair>{
		private int first;
		private int second;
		
		public void set(int left,int right){
			first=left;
			second=right;
		}
		public int getFirst(){
			return first;
		}
		public int getSecond(){
			return second;
		}
		
		public void readFields(DataInput in) throws IOException {//读
			first=in.readInt();
			second=in.readInt();
		}
		
		public void write(DataOutput out) throws IOException {//写
			out.writeInt(first);
			out.writeInt(second);
		}

		public int compareTo(IntPair arg) {//如果用户没有通过job.setSortComparatorClass()来设置排序类，则默认调用这个方法来对key排序
			if(first!=arg.first){
				return first-arg.first;//升序
			}
			else if(second!=arg.second){
				return arg.second-second;//降序
			}
			else return 0;
		}
		
	}