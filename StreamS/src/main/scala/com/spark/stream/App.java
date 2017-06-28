package com.pnc.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class App {
	private static final Logger logger = LoggerFactory.getLogger(App.class);
	private static final Logger kafkaLogger = LoggerFactory.getLogger("com.pnc.kafkaLogger");
	
	public static void main(String[] args) {
		int noofIterations=0;
		while(noofIterations<=10) {
		   for (int j=0;j<=10;j++) {
			   kafkaLogger.info("[Kafka Message="+j+"][Iteration="+noofIterations+"]");
		   }
     	   System.out.println("Sleeping");
     	   try {
     		   Thread.sleep(5000);
     	   } catch(InterruptedException e) {
     		   e.printStackTrace();
     	   }
     	  noofIterations++;
		}
	}
}