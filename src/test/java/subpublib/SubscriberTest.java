package subpublib;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import com.qxwz.ps.sp.IPubHandler;
import com.qxwz.ps.sp.Subscriber;
import com.qxwz.ps.sp.msg.PubMessage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SubscriberTest {
	
	ZkClient zkclient = new ZkClient("172.16.200.1:2181");
	
	IPubHandler handler = new IPubHandler() {

		@Override
		public void handlePubMessage(PubMessage msg) {
			log.info("收到数据:{}", msg);
		}
	};
	
	
	@Test
	public void sub1() throws InterruptedException
	{
		Subscriber sub1 = new Subscriber(zkclient, "/subpub_test");
		sub1.setPubHandler(handler);
		sub1.init();
//		sub1.setName("sub1");
		for(int i = 0; i < 10; ++ i)
		{
			sub1.subscribe(i + "");
		}
		 
		Thread.sleep(10000);
		
		for(int i = 0; i < 5; ++ i)
		{
			sub1.unsubscribe(i + "");
		}
		
		Thread.sleep(10000);
		
		for(int i = 0; i < 10; ++ i)
		{
			sub1.subscribe(i + "");
		}
		
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	@Test
	public void sub2()
	{
		Subscriber sub2 = new Subscriber(zkclient, "/subpub_test");
		sub2.setPubHandler(handler);
		sub2.init();
//		sub2.setName("sub2");
		for(int i = 5; i < 15; ++ i)
		{
			sub2.subscribe(i + "");
		}
		while(true)
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void sub3()
	{
		Subscriber sub3 = new Subscriber(zkclient, "/subpub_test");
		sub3.setPubHandler(handler);
		sub3.init();
//		sub3.setName("sub3");
		for(int i = 10; i < 20; ++ i)
		{
			sub3.subscribe(i + "");
		}
		while(true)
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	@Test
	public void sub11()
	{
		Subscriber sub = new Subscriber(zkclient, "/subpub_test");
		sub.setPubHandler(handler);
		sub.init();
		sub.subscribe("1");
		for(int i = 0 ; i < 20; ++ i)
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		sub.unsubscribe("1");
		while(true)
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	@Test
	public void sub22()
	{
		Subscriber sub = new Subscriber(zkclient, "/subpub_test");
		sub.setPubHandler(handler);
		sub.init();
		sub.subscribe("1");
		while(true)
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
