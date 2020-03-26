package subpublib;

import org.I0Itec.zkclient.ZkClient;

import com.qxwz.ps.sp.IPubHandler;
import com.qxwz.ps.sp.Subscriber;
import com.qxwz.ps.sp.msg.PubMessage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SubscriberTest {

	public static void main(String[] args) throws InterruptedException
	{
		ZkClient zkclient = new ZkClient("172.16.200.1:2181");
		
		IPubHandler handler = new IPubHandler() {

			@Override
			public void handlePubMessage(PubMessage msg) {
				log.info("收到数据:{}", msg);
			}
		};
	
		
		Subscriber sub1 = new Subscriber(zkclient, "/subpub_test");
		sub1.setPubHandler(handler);
		sub1.init();
		for(int i = 0; i < 10; ++ i)
		{
			sub1.subscribe(i + "");
		}
		
//		Subscriber sub2 = new Subscriber(zkclient, "/subpub_test");
//		sub2.setPubHandler(handler);
//		sub2.init();
//		for(int i = 5; i < 15; ++ i)
//		{
//			sub2.subscribe(i + "");
//		}
//		
//		Subscriber sub3 = new Subscriber(zkclient, "/subpub_test");
//		sub3.setPubHandler(handler);
//		sub3.init();
//		for(int i = 10; i < 20; ++ i)
//		{
//			sub3.subscribe(i + "");
//		}
	}

}
