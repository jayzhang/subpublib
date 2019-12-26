package com.qxwz.ps.sp;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.google.common.base.Joiner;
import com.qxwz.columbus.client.util.NetUtils;
import com.qxwz.ps.sp.msg.PubMessage;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
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
	
	@Setter @Getter
	private ISubHandler subHandler;
	
	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;
	
	private volatile Set<String> keys = new TreeSet<>(); // 订阅的所有key的集合
	private volatile Set<Channel> channels = new ConcurrentHashSet<>();
	
	private String mypath;
	private int msgNum = 0;

	public Publisher(ZkClient zkclient, String zkBasePath)
	{
		this.zkclient = zkclient;
		this.zkBasePath = zkBasePath;
	}
	
	public void init()
	{
		bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("Publisher-Boss")); 
		workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("Publisher-Worker"));
		ServerBootstrap b = new ServerBootstrap();
		
		String localhost = NetUtils.getLocalHost();
		mypath = zkBasePath + "/" + localhost+":"+port;
		 
        b.group(bossGroup, workerGroup)
        		.channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.TCP_NODELAY, true)  
                .childHandler(new PublisherInitializer(this));
        try {
			b.bind(localhost, port).sync();
			 log.info("绑定：{}:{}", host, port);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
        zkclient.delete(mypath);
		zkclient.createEphemeral(mypath);
		log.info("createEphemeral:{}", mypath);
		
		
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				
				while(true)
				{
					checkKeysChange();
					
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
		});
       
		t.start();
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
		Set<String> allKeys = new HashSet<>();
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
		for(Channel channel: channels)
		{
			Set<String> keys = channel.pipeline().get(PublisherChannelHandler.class).keys;
			if(keys.contains(key))
			{
				channel.writeAndFlush(pub);
				log.info("发送publish数据, key:{}, remote:{}, msgNum: {}", key, channel.remoteAddress(), ++ msgNum);
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
