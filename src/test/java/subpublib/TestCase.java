package subpublib;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.qxwz.ps.sp.IPubHandler;
import com.qxwz.ps.sp.ISubHandler;
import com.qxwz.ps.sp.Publisher;
import com.qxwz.ps.sp.Subscriber;
import com.qxwz.ps.sp.msg.PubMessage;
import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCase {

	public static final String ZK_ADDR = "172.16.200.1:2181";
	
	public static final String ZK_PATH = "/subpub_test";
	
	int nPubs = 10;
	int nSubs = 10;
	int nKeys = 100;  //key总数
	static final int dataFreq = 5000;
	Random random = new Random();
	
	@Slf4j
	static class MySubHandler implements ISubHandler
	{
		@Getter
		Map<String, Integer> subKeys = new ConcurrentHashMap<>();//记录key的订阅者个数
		
		public MySubHandler(Publisher pub)
		{
			Thread t = new Thread(()->{
				while(true)
				{
					try {
						Thread.sleep(dataFreq);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					pub.pubdataMock();
				}
			}) ;
			t.start();
		}
		
		public void close()
		{
			subKeys.clear();
		}
		
		@Override
		public void handleSubMessage(SubMessage msg) {
			log.info("【业务】收到订阅: {}", msg);
			subKey(msg.getKey());
			checkValid();
		}

		@Override
		public void handleUnsubMessage(UnsubMessage msg) {
			log.info("【业务】收到取消订阅: {}", msg);
			unsubKey(msg.getKey());
			checkValid();
		}
		
		public boolean subKey(String key)
		{
			Integer count = subKeys.getOrDefault(key, 0);
			subKeys.put(key, count + 1);
			if(count == 0) 
			{
				return true;
			}
			return false;
		}
		
		public boolean unsubKey(String key)
		{
			Integer count = subKeys.getOrDefault(key, 0);
			if(count == 0) return false;
			if(count == 1)
			{
				subKeys.remove(key);
				return true;
			}
			else
			{
				subKeys.put(key, count - 1);
				return false;
			}
		}
		
		/**
		 * 检查业务层没有重复订阅
		 */
		public void checkValid()
		{
			for(String k: subKeys.keySet())
			{
				Integer count = subKeys.get(k);
				assertTrue(count == 1);
			}
		}
	};
	
	
	@Slf4j
	static class MyPubHandler implements IPubHandler 
	{
		@Getter
		volatile Map<String, Long> keyTimeMap = new ConcurrentHashMap<>();
		
		@Override
		public void handlePubMessage(PubMessage msg) {
			log.info("收到数据:{}", msg);
			keyTimeMap.put(msg.getKey(), System.currentTimeMillis());
		}
		
		/**
		 * 检查订阅的keys是否正常在更新
		 */
		public boolean checkKeys(Set<String> keys, long timeout)
		{
			long now = System.currentTimeMillis();
			for(String k: keys)
			{
				Long time = keyTimeMap.get(k);
				if(time == null || now - time > timeout)
				{
					return false;
				}
			}
			return true;
		}
		
		public void close()
		{
			keyTimeMap.clear();
		}
	};
	
	List<Publisher> pubs = new ArrayList<>();
	
	List<Subscriber> subs = new ArrayList<>();
	
	
	String randomKey()
	{
		int r = random.nextInt(nKeys);
		return r + "";
	}
	
	List<String> randomKeys(int number)
	{
		List<String> keys = new ArrayList<>();
		for(int i = 0 ; i < number; ++ i)
		{
			int r = random.nextInt(nKeys);
			keys.add(r + "");
		}
		return keys;
	}
	
	Publisher getPublisherByChannel(Channel channel)
	{
		String remotePort = StringUtils.substringAfterLast(channel.remoteAddress().toString().substring(1), ":");
		int port = Integer.parseInt(remotePort);
		int index = port - 29000;
		Publisher pub = pubs.get(index);
		return pub;
	}
	
	boolean haveLivePublishers()
	{
		for(Publisher pub: pubs)
		{
			if( !pub.isClosed() )
				return true;
		}
		return false;
	}
	
	
	@Test
	public void publisher()
	{
		for(int i = 0 ; i < nPubs; ++ i)
		{
			Publisher pub = new Publisher(ZK_ADDR, ZK_PATH);
			pub.setPort(29000 + i);
			pub.setSubHandler(new MySubHandler(pub));
			pub.init();
			pubs.add(pub);
		}
		while(true)
		{
			for(int i = 0 ; i < nPubs; ++ i)
			{
				Publisher pub = pubs.get(i);
				double sample = random.nextDouble();
				if(sample < 0.1)
				{
					if(pub.isClosed())
					{
						pub.init();
					}
					else 
					{
//						handler.close();
//						pub.close();
					}
					try {
						Thread.sleep(dataFreq);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			try {
				Thread.sleep(dataFreq);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void subscriber()
	{
		for(int i = 0 ; i < nSubs; ++ i)
		{
			Subscriber sub = new Subscriber(ZK_ADDR, ZK_PATH);
			sub.setPubHandler(new MyPubHandler());
			sub.init();
			sub.setWorkerThreadNum(1);
			subs.add(sub);
		}
		int r = 0;
		while(true)
		{
			log.info("--------------------Run: {}", r ++);
			for(int i = 0 ; i < nSubs; ++ i)
			{
				Subscriber sub = subs.get(i);
				MyPubHandler handler = (MyPubHandler) sub.getPubHandler();

				double sample = random.nextDouble();
				
				if(sample >= 0.4) //订阅
				{
					if(sub.isClosed())
					{
						sub.init();
					}
					String key = randomKey();
					sub.subscribe(key); 
				}
				else if(sample >= 0.1)//取消订阅
				{
					if(sub.isClosed())
					{
						sub.init();
					}
					String key = randomKey();
					sub.unsubscribe(key);
				}
				else 
				{
					if(sub.isClosed())
					{
						sub.init();
					}
					else 
					{
						handler.close();
						sub.close();
					}
				}
				try {
					Thread.sleep(dataFreq);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				boolean havePub = haveLivePublishers();
				boolean isok = handler.checkKeys(sub.getSubscribedKeys(), dataFreq + 1000);
				if(havePub && !isok)
				{
					System.out.println();
				}
				assertTrue(!havePub || isok); //检查每个key的消息时间是最新的
				 
			}
		}
	}
}
