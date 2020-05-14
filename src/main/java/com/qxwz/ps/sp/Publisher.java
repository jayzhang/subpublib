package com.qxwz.ps.sp;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Joiner;
import com.qxwz.ps.sp.msg.PubMessage;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
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
	private int syncZkInterval = 10;//检查并同步订阅关系到ZK的频率，单位秒
	@Setter
	private int bossThreadNum = 1;
	@Setter
	private int workerThreadNum = 0; //use netty default workerThreadNum NCPU<<1

	@Setter @Getter
	private ISubHandler subHandler;
	
	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;
	
	private volatile Set<String> keys = new TreeSet<>(); // 订阅的所有key的集合
	private volatile Set<Channel> channels = new HashSet<>();
	private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("Publisher-SubKeysSyncer-"));
	
	private String mypath;
	
	@Setter
	private String name;

	public Publisher(ZkClient zkclient, String zkBasePath)
	{
		this.zkclient = zkclient;
		this.zkBasePath = zkBasePath;
	}
	
	synchronized public void init()
	{
		log.info("start to init Pubsublib Publisher.....");
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
                .childHandler(new PublisherInitializer(this));

		b.bind(localhost, port).addListener(future -> {
			ChannelFuture channelFuture = (ChannelFuture) future;
			if (channelFuture.isSuccess()) {
				log.info("Publisher start with bossThreadNum:{} workerThreadNum:{} bind port:{}",bossThreadNum,workerThreadNum,port);
				zkclient.delete(mypath);
				zkclient.createEphemeral(mypath);
				log.info("createEphemeral:{}", mypath);
				executorService.scheduleWithFixedDelay(()->checkKeysChange(),syncZkInterval,syncZkInterval, TimeUnit.SECONDS);
				log.info("Publisher init success");
			}else {
				log.warn("Publisher start failed.....");
				if (channelFuture.cause()!=null) {
					log.error("Publisher start failed, cause:{}", channelFuture.cause());
				}
			}
		});
	}
	
	public void checkKeysChange()
	{
		Set<String> keys = new TreeSet<>();
		for(Channel ch: channels)
		{
			keys.addAll(ch.pipeline().get(PublisherChannelHandler.class).keys);
		}
		String oldKeys = Joiner.on(",").join(this.keys);
		String newKeys = Joiner.on(",").join(keys);
		if(!StringUtils.equals(oldKeys, newKeys))// 总key发生变化
		{
			this.keys = keys;
			zkclient.writeData(mypath, newKeys);
			log.info("key发生变化, 写zk, path:{}, data:{}", mypath, newKeys);
		}
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
		Set<String> allKeys = new TreeSet<>();
		for(Channel channel: channels)
		{
			Set<String> keys = channel.pipeline().get(PublisherChannelHandler.class).keys;
			allKeys.addAll(keys);
		}
		for(String key: allKeys)
		{
			publish(key, key.getBytes());
		}
	}
	
	public void publish(String key, byte[] data)
	{
		PubMessage pub = new PubMessage(key, data);
//		pub.setPublisherName(name);
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
	
	public void close()
	{
		if(bossGroup != null)
		{
			bossGroup.shutdownGracefully();
		}
		if(workerGroup != null)
		{
			workerGroup.shutdownGracefully();
		}
	}
}
