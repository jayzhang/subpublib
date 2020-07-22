package com.qxwz.ps.sp;

import java.util.HashSet;
import java.util.Set;

import com.qxwz.ps.sp.msg.Message;
import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PublisherChannelHandler extends ChannelInboundHandlerAdapter{

	private Publisher publisher;
	
//	@Setter @Getter
//	private String subsriberName;
	
	public volatile Set<String> keys = new HashSet<>(); // 订阅的所有key的集合
	
	public PublisherChannelHandler(Publisher publisher)
	{
		this.publisher = publisher;
	}
	
	
	@Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
		log.info("channelActive: {}", ctx.channel());
		publisher.addChannel(ctx.channel());
    }
	@Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		log.info("channelInactive: {}", ctx.channel());
		publisher.removeChannel(ctx.channel());
    }
	
	
	@Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception 
	{
		log.info("收到消息:{}, ch:{}, 更新前被订阅的keys:{}", msg, ctx.channel(), keys);
        Message message = (Message) msg;
        ISubHandler subHandler = publisher.getSubHandler();
        if(message instanceof SubMessage)
        {
        	SubMessage sub = (SubMessage)message;
        	String key = sub.getKey();
        	if(key != null)
        	{
        		if(!keys.contains(key))
                {
        			if(subHandler != null)
        	    	{
        				Set<String> keys = publisher.subsbribedKeysInAllActiveChannels();
        				if(!keys.contains(key))	//第一次订阅该key
        				{
        					subHandler.handleSubMessage(sub);
        	        	}
        				else 
        				{
        					log.info("key:{}被channel:{}订阅，但是因为其他channel已经订阅该key，因此不触发订阅事件!", key, ctx.channel());
        				}
        			}
                	keys.add(key);
                }
        	}
        }
        else if(message instanceof UnsubMessage)
        {
        	UnsubMessage unsub = (UnsubMessage)message;
        	String key = unsub.getKey();
        	if(key != null)
        	{
        		if(keys.contains(key))
            	{
            		keys.remove(key);
            		if(subHandler != null)
                	{
            			Set<String> keys = publisher.subsbribedKeysInAllActiveChannels();
            			if(!keys.contains(key))	//取消订阅之后无人订阅
            			{
            				subHandler.handleUnsubMessage(unsub);
            			}
            			else 
            			{
            				log.info("key:{}被channel:{}取消订阅，但是因为其他channel正在订阅该key，因此不触发取消订阅事件!", key, ctx.channel());
            			}
                	}
            	}
        	}
        }
        log.info("更新后的keys:{}", keys);
    }
	
}
