package com.qxwz.ps.sp;

import java.util.HashSet;
import java.util.Set;

import com.qxwz.ps.sp.msg.Message;
import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PublisherChannelHandler extends ChannelInboundHandlerAdapter{

	private Publisher publisher;
	
	@Setter @Getter
	private String subsriberName;
	
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
        if(message instanceof SubMessage)
        {
        	SubMessage sub = (SubMessage)message;
        	this.subsriberName = sub.getSubscriberName();
        	String key = sub.getKey();
        	if(key != null)
        	{
        		if(!keys.contains(key))
                {
                	keys.add(key);
                	if(publisher.getSubHandler() != null)
                	{
                		publisher.getSubHandler().handleSubMessage(sub);
                	}
                }
        	}
        }
        else if(message instanceof UnsubMessage)
        {
        	UnsubMessage unsub = (UnsubMessage)message;
        	String key = unsub.getKey();
        	if(keys.contains(key))
        	{
        		keys.remove(key);
        		if(publisher.getSubHandler() != null)
        		{
        			publisher.getSubHandler().handleUnsubMessage(unsub);
        		}
        	}
        }
        log.info("更新后的keys:{}", keys);
    }
	
}
