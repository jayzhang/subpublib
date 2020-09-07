package subpublib;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import com.qxwz.ps.sp.ISubHandler;
import com.qxwz.ps.sp.Publisher;
import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PublishTest {
	
	ZkClient zkclient = new ZkClient("172.16.200.1:2181");
	
	ISubHandler handler = new ISubHandler() {

		@Override
		public void handleSubMessage(SubMessage msg) {
			log.info("【业务】收到订阅: {}", msg);
		}

		@Override
		public void handleUnsubMessage(UnsubMessage msg) {
			log.info("【业务】收到取消订阅: {}", msg);
		}
	};
	
	@Test 
	public void pub1() throws InterruptedException
	{
		Publisher pub = new Publisher(zkclient, "/subpub_test");
		pub.setPort(29001);
		pub.setSubHandler(handler);
		pub.init();
		
		while(true)
		{
			pub.pubdataMock();
			Thread.sleep(10000);
		}
	}
	
	@Test 
	public void pub2() throws InterruptedException
	{
		Publisher pub = new Publisher(zkclient, "/subpub_test");
		pub.setPort(29002);
		pub.setSubHandler(handler);
		pub.init();
		
		while(true)
		{
			pub.pubdataMock();
			Thread.sleep(10000);
		}
	}
	
	@Test 
	public void pub3() throws InterruptedException
	{
		Publisher pub = new Publisher(zkclient, "/subpub_test");
		pub.setPort(29003);
		pub.setSubHandler(handler);
		pub.init();
		
		while(true)
		{
			pub.pubdataMock();
			Thread.sleep(1000);
		}
	}
	
}
