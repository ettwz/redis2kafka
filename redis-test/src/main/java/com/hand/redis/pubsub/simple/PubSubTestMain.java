package com.hand.redis.pubsub.simple;

import com.hand.redis.Constants;
import com.hand.kafka.producer.kafkaProducer;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.JedisPubSub;



@RestController
public class PubSubTestMain {

	@RequestMapping(value = "cli")
	public String cli() throws InterruptedException {
		final String channel = "test";
		Thread.sleep(2000);
		//消息订阅着非常特殊，需要独占链接，因此我们需要为它创建新的链接；
		//此外，jedis客户端的实现也保证了“链接独占”的特性，sub方法将一直阻塞，
		//直到调用listener.unsubscribe方法
		Thread subThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try{
					SubClient subClient = new SubClient(Constants.host, Constants.port);
					System.out.println("----------subscribe operation begin-------");
					JedisPubSub listener = new PrintListener();
					//在API级别，此处为轮询操作，直到unsubscribe调用，才会返回
					subClient.sub(listener,channel);
					System.out.println("----------subscribe operation end-------");
				}catch(Exception e){
					e.printStackTrace();
				}

			}
		});
		subThread.start();

		subThread.interrupt();

		return "subclient open";
	}


}
