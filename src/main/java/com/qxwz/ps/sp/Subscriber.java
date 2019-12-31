package com.qxwz.ps.sp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qxwz.ps.sp.msg.Message;
import com.qxwz.ps.sp.msg.PubMessage;
import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Sharable
public class Subscriber extends ChannelInboundHandlerAdapter implements IZkChildListener, IZkDataListener{

	private String zkBasePath;	
	private ZkClient zkclient;
	private EventLoopGroup workerGroup;
	private Bootstrap clientbootstrap;
	private volatile Map<String, Channel> channelMap = new HashMap<>(); // channel address ----> channel
	private volatile Map<String, Set<String>> routerMap = new HashMap<>(); // channel address ----> key set
	private volatile Map<String, Channel> key2channel = new HashMap<>();  // key------>channel
	private int msgNum = 0;
	
	@Setter
	private IPubHandler pubHandler;
	
	
	public Subscriber(ZkClient zkclient, String zkBasePath)
	{
		this.zkclient = zkclient;
		this.zkBasePath = zkBasePath;
	}
	
	
	public void init()
	{
		workerGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("Subscriber-Worker"));
		clientbootstrap = new Bootstrap();
		clientbootstrap.group(workerGroup).channel(NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY, true)  
        .handler(new SubscriberInitializer(this));
		zkclient.subscribeChildChanges(zkBasePath, this);
		List<String> children = zkclient.getChildren(zkBasePath);
		try {
			handleChildChange(zkBasePath, children);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	@Override
	public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception 
	{
		log.info("handleChildChange, parentPath:{}, currentChilds:{}", parentPath, currentChilds);
		Set<String> toDel = new HashSet<>();
		for(String addr: channelMap.keySet())
		{
			if(!currentChilds.contains(addr))
			{
				channelMap.get(addr).close();
				log.info("pub结点下线，关闭连接:{}", addr);
				toDel.add(addr);
			}
		}
		for(String k: toDel)
		{
			channelMap.remove(k);
		}
		for(String child: currentChilds )
		{
			if(!channelMap.containsKey(child))
			{
				Channel channel = connect(child);
				if(channel != null)
				{
					channelMap.put(child, channel);
					String dataPath = parentPath + "/" + child;
					zkclient.subscribeDataChanges(dataPath, this);  //订阅数据变化
					Object data = zkclient.readData(dataPath, true);
					handleDataChange(dataPath, data); //初始化读取路由表
					log.info("新的pub上线，subscribeDataChanges, path:{}", parentPath + "/" + child);
				}
			}
		}
	}
	
	private Channel connect(String addr)
	{
		List<String> arr = Splitter.on(":").splitToList(addr);
		try {
			ChannelFuture future = clientbootstrap.connect(arr.get(0), Integer.parseInt(arr.get(1))).sync();
			return future.channel();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}


	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		log.info("handleDataChange, dataPath:{}, data:{}", dataPath, data);
		String addr = StringUtils.substringAfterLast(dataPath, "/");
		if(data != null)
		{
			String str = (String)data;
			List<String> keylist = Splitter.on(",").omitEmptyStrings().splitToList(str);
			Set<String> keys = Sets.newHashSet(keylist);
			routerMap.put(addr, keys);
		}
		else
		{
			routerMap.put(addr, new HashSet<>());
		}
	}


	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		log.info("handleDataDeleted, dataPath:{}", dataPath);
		String addr = StringUtils.substringAfterLast(dataPath, "/");
		routerMap.remove(addr);
	}
	
	public void subscribe(String key)
	{
		log.info("尝试订阅数据, key:{}", key);
		List<String> existingList = new ArrayList<>();
		int min = Integer.MAX_VALUE;
		String idlePub = null;
		
		int existingMin = Integer.MAX_VALUE;
		String existingIdlePub = null;
		
		List<String> addrs = Lists.newArrayList(routerMap.keySet());
		
		Collections.shuffle(addrs);
		
		for(String addr: addrs)
		{
			Set<String> keys = routerMap.get(addr);
			if(keys.contains(key))
			{
				existingList.add(addr);
				if(existingMin > keys.size())
				{
					existingMin = keys.size();
					existingIdlePub = addr;
				}
			}
			if(min > keys.size())
			{
				min = keys.size();
				idlePub = addr;
			}
		}
		Channel channel = null;
		if(existingList.size() == 0)  //没有现存订阅的结点，随机选一台pub
		{
			channel = channelMap.get(idlePub);
			log.info("key:{}未订阅过，挑选出最空闲的pub: {}, 负载:{}", key, idlePub, min);
		}
		else //有现存订阅的结点，在现存中随机选一台pub
		{
			channel = channelMap.get(existingIdlePub);
			log.info("key:{}已订阅过，挑选出最空闲的pub: {}, 负载:{}", key, existingIdlePub, existingMin);
		}
		if(channel != null)
		{
			SubMessage sub = new SubMessage(key);
			channel.writeAndFlush(sub);
			key2channel.put(key, channel);
			log.info("向{}订阅数据:{}", channel.remoteAddress(), key);
		}
		else 
		{
			log.error("没有pub可订阅!");
		}
	}
	
	public void close()
	{
		if(workerGroup != null)
		{
			workerGroup.shutdownGracefully();
		}
	}
	
	private void transferKeys(Channel channel)
	{
		String addr = null;
		
		for(Entry<String, Channel> entry: channelMap.entrySet())
		{
			if(entry.getValue() == channel)
			{
				addr = entry.getKey();
				break;
			}
		}
		
		if(addr != null)
		{
			channelMap.remove(addr);
			routerMap.remove(addr);	
		}
		
		Set<String> keys = new HashSet<>();
		for(Entry<String, Channel> entry: key2channel.entrySet())
		{
			if(entry.getValue() == channel)
			{
				keys.add(entry.getKey());
			}
		}
		log.info("故障转移的key:{}", keys);
		for(String key: keys)
		{
			this.subscribe(key);
		}
	}
	
	@Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		transferKeys(ctx.channel());
    }
	
	public void unsubscribe(String key)
	{
		log.info("尝试取消订阅数据, key:{}", key);
		Channel channel = key2channel.get(key);
		if(channel != null)
		{
			UnsubMessage sub = new UnsubMessage(key);
			channel.writeAndFlush(sub);
			key2channel.remove(key);
			log.info("向{}取消订阅数据:{}", channel.remoteAddress(), key);
		}
	}
	
	@Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Message message = (Message)msg;
		if(message  instanceof PubMessage)
		{
			PubMessage pub = (PubMessage)message;
			log.info("收到订阅的数据, key:{}, value:{}, remote:{}, msgNum:{}"
					, pub.getKey(), new String(pub.getData()), ctx.channel().remoteAddress(), ++ msgNum);
			
			if(pubHandler != null)
			{
				pubHandler.handlePubMessage(pub);
			}
		}
    }
}
