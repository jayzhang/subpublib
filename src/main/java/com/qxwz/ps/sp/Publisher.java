package com.qxwz.ps.sp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.qxwz.ps.sp.msg.Message;
import com.qxwz.ps.sp.msg.PubMessage;
import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.SyncMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class Publisher{

	@Setter
	private String zkBasePath;
	@Setter
	private String host = "0.0.0.0";
	@Setter
	private int port = 29001;
	@Setter
	private ZkClient zkclient;
	@Setter
	private int syncZkInterval = 60;//检查并同步订阅关系到ZK的频率，单位秒
	@Setter
	private int bossThreadNum = 1;
	@Setter
	private int workerThreadNum = 0; //use netty default workerThreadNum NCPU<<1
	@Setter
	private int lowWriteBufferWaterMark = 32*1024;
	@Setter
	private int highWriteBufferWaterMark = 128*1024;

	@Setter @Getter
	private ISubHandler subHandler;
	
	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;
	
	private volatile Map<String, Integer> keys = new HashMap<>(); // 当前订阅的所有key被订阅的链路数
	private volatile Set<Channel> channels = Sets.newConcurrentHashSet(); // 当前所有连接
	
//	private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("Publisher-SubKeysSyncer-"));
	
	private LinkedBlockingQueue<Message> msgQueue = new LinkedBlockingQueue<>(1000000);
	
	private Thread zkSyncThread;
	
	private int batchSyncSize = 1000;
	private int batchSyncSecs = 2;
	private volatile boolean zkSyncThreadStart = true;
	
	@Getter
	private volatile boolean closed = true;
	
	private String mypath;
	
	public Publisher(ZkClient zkclient, String zkBasePath)
	{
		this.zkclient = zkclient;
		this.zkBasePath = zkBasePath;
		startZkSyncThread();
	}
	
	public Publisher(String zkServerAddress, String zkBasePath)
	{
		this.zkclient = new ZkClient(zkServerAddress);
		this.zkBasePath = zkBasePath;
		startZkSyncThread();
	}
	
	private void startZkSyncThread()
	{
		int maxFetchNum = Math.max(syncZkInterval/batchSyncSecs, 1);
		zkSyncThread = new Thread(()->
		{
			int fetchNum = 0;
			while(true)
    		{
				if(zkSyncThreadStart)
				{
					List<Message> list = new ArrayList<>();
	        		while(true)
	        		{
	    				try {
	    					Message msg = msgQueue.poll(batchSyncSecs, TimeUnit.SECONDS);
	    					if(msg != null)
	    	        		{
	    	        			list.add(msg);
	    	        			if(list.size() == batchSyncSize) //一直有数据，但是list满了，需要清掉
	    	        			{
	    	        				break;
	    	        			}
	    	        		}
	    					else  //等够batchSyncSecs秒，没数据了
	    					{
	    						break;
	    					}
	    				} catch (InterruptedException e) {
	    					// TODO Auto-generated catch block
	    					e.printStackTrace();
	    				}
	        		}
	        		
	        		if(list.size() > 0)
	        		{
	        			try
	        			{
	        				log.info("处理订阅/反订阅消息, size:{}", list.size());
	        				
	        				boolean syncWhole = list.size() == 1 && list.get(0) instanceof SyncMessage;
	        				if(syncWhole) //全量检查
	        				{
	        					wholeCheck();
	        				}
	        				else //增量检查
	        				{
	        					deltaCheck(list);
	        				}
	        			}
	        			finally
	        			{
	        				list.clear();
	        			}
	        		}
	        		else //同步全量检查的时机：累积到maxFetchNum次没有增量消息时，进行一次全量同步
	        		{
	        			log.debug("tick!");
	        			++ fetchNum;
	        			if(fetchNum >= maxFetchNum)
	        			{
	        				triggerWholeSync();
	        				fetchNum = 0;
	        			}
	        		}
				}
				else 
				{
					try {
						Thread.sleep(batchSyncSecs * 1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
    		}
		}, "SubKeysZkSyncer");
		zkSyncThread.start();
		log.info("启动Publisher-SubKeysZkSyncer线程!");
	}
	
	synchronized public void init()
	{
		log.info("@@@@@@@@@@@@@启动发布!");
		if(!zkclient.exists(zkBasePath))
		{
			zkclient.createPersistent(zkBasePath,true);
		}
		
		bossGroup = new NioEventLoopGroup(bossThreadNum, new DefaultThreadFactory("Publisher-Boss"));
		workerGroup = new NioEventLoopGroup(workerThreadNum, new DefaultThreadFactory("Publisher-Worker"));
		ServerBootstrap b = new ServerBootstrap();
		
		String localhost = NetUtils.getLocalHost();
		
		mypath = zkBasePath + "/" + localhost+":"+port;
		 
        b.group(bossGroup, workerGroup)
        		.channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.TCP_NODELAY, true)
				.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(lowWriteBufferWaterMark,highWriteBufferWaterMark))
                .childHandler(new PublisherInitializer(this));

		b.bind(localhost, port).addListener(future -> {
			ChannelFuture channelFuture = (ChannelFuture) future;
			if (channelFuture.isSuccess()) {
				log.info("Publisher start with bossThreadNum:{} workerThreadNum:{} bind port:{}",bossThreadNum,workerThreadNum,port);
				zkclient.delete(mypath);
				zkclient.createEphemeral(mypath);
				log.info("createEphemeral:{}", mypath);
//				executorService.scheduleWithFixedDelay(()->triggerWholeSync(),syncZkInterval,syncZkInterval, TimeUnit.SECONDS);
				log.info("Publisher init success");
				closed = false;
			}else {
				log.warn("Publisher start failed.....");
				if (channelFuture.cause()!=null) {
					log.error("Publisher start failed, cause:{}", channelFuture.cause());
				}
			}
		});
	}
	
	
	
	/**
	 * 全量检查所有channel订阅的key和本地keys差异，同时比较跟zk的差异，最后同步到zk
	 */
	private void wholeCheck()
	{
		log.info("wholeCheck start!");
		Set<String> newKeys = new HashSet<>();
		Map<String, Integer> newKeysMap = new HashMap<>();
		for(Channel ch: channels)
		{
			if(ch.isActive())
			{
				Set<String> channelKeys = ch.pipeline().get(PublisherChannelHandler.class).keys;
				log.debug("channel:{}, keys:{}", ch, channelKeys);
				newKeys.addAll(channelKeys);
				for(String k : newKeys)
				{
					newKeysMap.put(k, newKeysMap.getOrDefault(k, 0) + 1);
				}
			}
		}
		Set<String> oldKeys = this.keys.keySet();
		SetView<String> diff = Sets.symmetricDifference(oldKeys, newKeys);
		if(diff.size() > 0 && subHandler != null)
		{
			for(String k: diff)
			{
				if(oldKeys.contains(k)) //remove
				{
					SubMessage msg = new SubMessage();
					msg.setKey(k);
					subHandler.handleSubMessage(msg);
				}
				else if(newKeys.contains(k))//add 
				{
					UnsubMessage msg = new UnsubMessage();
					msg.setKey(k);
					subHandler.handleUnsubMessage(msg);
				}
			}
		}
		this.keys = newKeysMap;
		String zkData = (String)zkclient.readData(mypath);
		List<String> zklist = StringUtils.isEmpty(zkData) ? Lists.newArrayList() : Splitter.on(",").splitToList(zkData);
		Set<String> zkKeys = Sets.newHashSet(zklist);
		log.debug("####zkKeys:{}, localKeys:{}", zkKeys, newKeys);
		SetView<String> diffWithZK = Sets.symmetricDifference(zkKeys, newKeys);
		if(diffWithZK.size() > 0)// 总key发生变化
		{
			String newZkData = Joiner.on(",").join(newKeys);
			zkclient.writeData(mypath, newZkData);
			log.info("订阅的keys发生变化, 写zk, path:{}, keys size:{}, data:{}", mypath, newKeys.size(), newZkData);
		}
		log.info("wholeCheck finish! diffWithZK.size={}", diffWithZK.size());
	}
	
	/**
	 * 增量比较变化，并同步到zk
	 */
	private void deltaCheck(List<Message> list)
	{
		log.debug("deltaCheck start! list.size:{}", list.size());
		Map<String, Integer> delta = new HashMap<>();
		Set<String> addKeys = new HashSet<>();
		Set<String> removeKeys = new HashSet<>();
		for(Message msg: list)
		{
			if(msg instanceof SubMessage)
			{
				String k = ((SubMessage) msg).getKey();
				Integer subCount = keys.getOrDefault(k,0);
				if(subCount == 0)
				{
					addKeys.add(k);
				}
				keys.put(k, subCount + 1);
				delta.put(k, delta.getOrDefault(k,0) + 1);
			}
			else if(msg instanceof UnsubMessage)
			{
				String k = ((UnsubMessage) msg).getKey();
				Integer subCount = keys.getOrDefault(k,0);
				if(subCount <= 1)
				{
					keys.remove(k);
					removeKeys.add(k);
				}
				else 
				{
					keys.put(k, subCount - 1);
				}
				delta.put(k, delta.getOrDefault(k,0) - 1);
			}
		}
		Set<String> unchangedKeys = new HashSet<>();
		for(String k: delta.keySet())
		{
			Integer count = delta.get(k);
			if(count == 0)
			{
				unchangedKeys.add(k);
			}
		}
		addKeys.removeAll(unchangedKeys);
		removeKeys.removeAll(unchangedKeys);
		if(addKeys.size() > 0 || removeKeys.size() > 0) //订阅keys发生变化
		{
			String keystr = Joiner.on(",").join(keys.keySet());
			zkclient.writeData(mypath, keystr);
			log.info("订阅的keys发生变化, 写zk, path:{}, data:{}", mypath, keystr);
			if(subHandler != null)
			{
				for(String k: addKeys)
				{
					SubMessage msg = new SubMessage();
					msg.setKey(k);
					subHandler.handleSubMessage(msg);
				}
				for(String k: removeKeys)
				{
					UnsubMessage msg = new UnsubMessage();
					msg.setKey(k);
					subHandler.handleUnsubMessage(msg);
				}
			}
		}
		log.debug("deltaCheck finish! add:{} remove:{}", addKeys.size(), removeKeys.size());
	}
	
	public void triggerWholeSync()
	{
		msgQueue.add(new SyncMessage());
	}
	
	public void addChannel(Channel channel)
	{
		channels.add(channel);
	}
	public void removeChannel(Channel channel)
	{
		channels.remove(channel);
	}
	
	///模拟发送数据
	public void pubdataMock()
	{
		for(String key: keys.keySet())
		{
			publish(key, key.getBytes());
		}
	}
	
	public void publish(String key, byte[] data)
	{
		PubMessage pub = new PubMessage(key, data);
		for(Channel channel: channels)
		{
			if (channel==null) return;
			Set<String> keys = channel.pipeline().get(PublisherChannelHandler.class).keys;
			if(keys.contains(key))
			{
				if(channel.isWritable()) {
					channel.writeAndFlush(pub);
					log.info("发送publish数据, key:{}, remote:{}", key, channel.remoteAddress());
				}else {
					Integer currentHighWaterMark = channel.config().getWriteBufferHighWaterMark();
					Integer currentSendBuf = channel.config().getOption(ChannelOption.SO_SNDBUF);
					log.warn("channel:{} can not write message into channelBuffer" +
							",currentHighWaterMark:{},currentSendBuf:{}",channel,currentHighWaterMark,currentSendBuf);
				}
			}
		}
	}
	 
	public void onMessage(Message msg)
	{
		msgQueue.add(msg);
	}
	
	public void handleUnsubKeys(Set<String> removingkeys)
	{
		for(String k: removingkeys)
		{
			UnsubMessage msg = new UnsubMessage();
			msg.setKey(k);
			msgQueue.add(msg);
		}
	}
	
	public void close()
	{
		log.info("################关闭发布!");
		zkSyncThreadStart = false;
		
		if(bossGroup != null)
		{
			bossGroup.shutdownGracefully();
		}
		if(workerGroup != null)
		{
			workerGroup.shutdownGracefully();
		}
		
		keys.clear();
		channels.clear();
		msgQueue.clear();
		
		closed = true;
	}
}
