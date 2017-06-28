package com.pnc.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaLog {
	static Logger defaultLogger = LoggerFactory.getLogger(KafkaLog.class);
	static Logger kafkaLogger = LoggerFactory.getLogger("com.example.kafkaLogger");
	
	public static void main(String arg[]){
		int maxIterations =10;
		int noofIterations = 0;
		for (int i=0; i<=5;i++) {
			kafkaLogger.info("str");
			if (noofIterations < maxIterations) {
	     	   System.out.println("Exiting at i= " + i);
	     	   break;
	        } else {
	         	   System.out.println("Sleeping at i= " + i);
	         	   try {
	         		   Thread.sleep(5000);
	         	   } catch(InterruptedException e) {
	         		   e.printStackTrace();
	         	   }

	        }
			noofIterations++;
		}
	}
}
