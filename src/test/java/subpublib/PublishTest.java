package subpublib;

import org.I0Itec.zkclient.ZkClient;

import com.qxwz.ps.sp.ISubHandler;
import com.qxwz.ps.sp.Publisher;
import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PublishTest {

	public static void main(String[] args) throws InterruptedException {
		
		ZkClient zkclient = new ZkClient("172.16.200.1:2181");
		
		ISubHandler handler = new ISubHandler() {

			@Override
			public void handleSubMessage(SubMessage msg) {
				log.info("订阅: {}", msg);
			}

			@Override
			public void handleUnsubMessage(UnsubMessage msg) {
				log.info("取消订阅: {}", msg);
			}
		};
		
		Publisher pub1 = new Publisher(zkclient, "/subpub_test");
		pub1.setPort(29001);
		pub1.setSubHandler(handler);
		pub1.init();
		
		Publisher pub2 = new Publisher(zkclient, "/subpub_test");
		pub2.setPort(29002);
		pub2.setSubHandler(handler);
		pub2.init();
		
		Publisher pub3 = new Publisher(zkclient, "/subpub_test");
		pub3.setPort(29003);
		pub3.setSubHandler(handler);
		pub3.init();
		
		while(true)
		{
			pub1.pubdataMock();
			pub2.pubdataMock();
			pub3.pubdataMock();
			Thread.sleep(1000);
		}
	}

}
