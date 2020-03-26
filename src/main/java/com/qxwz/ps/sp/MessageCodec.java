package com.qxwz.ps.sp;

import java.util.List;

import com.qxwz.ps.sp.msg.HeartbeatMessage;
import com.qxwz.ps.sp.msg.Message;
import com.qxwz.ps.sp.msg.MessageTypes;
import com.qxwz.ps.sp.msg.PubMessage;
import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;


@Sharable
public class MessageCodec extends MessageToMessageCodec<ByteBuf,Message>{

	@Override
	protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
		ByteBuf buf = ctx.alloc().buffer();
		msg.encodeHead(buf);
		msg.encodeBody(buf);
		out.add(buf);
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
		int cmd = msg.getUnsignedByte(0);
		Message message = null;
		if(cmd == MessageTypes.SUB)
		{
			message = new SubMessage();
		}
		else if(cmd == MessageTypes.PUB)
		{
			message = new PubMessage();
		}
		else if(cmd == MessageTypes.HB)
		{
			message = new HeartbeatMessage();
		}
		else if(cmd == MessageTypes.UNSUB)
		{
			message = new UnsubMessage();
		}
		message.decodeHead(msg);
		message.decodeBody(msg);
		out.add(message);
	}

}
