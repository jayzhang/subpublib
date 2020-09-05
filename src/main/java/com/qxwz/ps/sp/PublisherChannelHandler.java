package com.qxwz.ps.sp;

import java.util.Set;

import com.google.common.collect.Sets;
import com.qxwz.ps.sp.msg.Message;
import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PublisherChannelHandler extends ChannelInboundHandlerAdapter{

	private Publisher publisher;
	
	public Set<String> keys = Sets.newConcurrentHashSet(); // 订阅的所有key的集合
	
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
		log.info("尝试取消所有channel:{}订阅的keys:{}", ctx.channel(), keys);
		publisher.removeChannel(ctx.channel());
		publisher.handleUnsubKeys(keys);
    }
	 
	@Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception 
	{
		log.info("channelRead start! 收到消息:{}, ch:{}, 当前keys:{}", msg, ctx.channel(), keys);
        Message message = (Message) msg;
        if(message instanceof SubMessage)
        {
        	SubMessage sub = (SubMessage)message;
        	String key = sub.getKey();
        	if(key != null)
        	{
        		if(!keys.contains(key))
                {
        			keys.add(key);
        			publisher.onMessage(message);
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
        			publisher.onMessage(message);
            	}
        	}
        }
        log.info("channelRead finish! ch:{}, keys:{}", ctx.channel(), keys);
    }
	
}
