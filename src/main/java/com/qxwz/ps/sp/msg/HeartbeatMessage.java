package com.qxwz.ps.sp.msg;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class HeartbeatMessage extends Message{

	 
	public HeartbeatMessage()
	{
		super.setCmd(MessageTypes.HB);
		super.setSeq(Message.SEQ.incrementAndGet());
	}

	@Override
	public void encodeBody(ByteBuf buf) {
		 
	}

	@Override
	public void decodeBody(ByteBuf buf) {
		 
	}
}
