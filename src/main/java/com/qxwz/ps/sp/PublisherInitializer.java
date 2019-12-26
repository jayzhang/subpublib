package com.qxwz.ps.sp;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class PublisherInitializer extends ChannelInitializer<SocketChannel>
{
    private Publisher publisher;
    
    public PublisherInitializer(Publisher publisher)
    {
        this.publisher = publisher;
    }
    
    @Override
    protected void initChannel(SocketChannel ch) throws Exception
    {
        ChannelPipeline p = ch.pipeline();
//        p.addLast( new LoggingHandler(LogLevel.INFO));
        p.addLast( new LengthFieldBasedFrameDecoder(4 * 1024, 0, 4, 0, 4)); //inbound
        p.addLast( new LengthFieldPrepender(4, false));						//outbound       
        p.addLast( new MessageCodec()); //inbound
        
//        p.addLast(new StringDecoder());
//        p.addLast(new StringEncoder());
        p.addLast(new PublisherChannelHandler(publisher));
    }
}
