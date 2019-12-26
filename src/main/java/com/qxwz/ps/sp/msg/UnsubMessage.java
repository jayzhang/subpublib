package com.qxwz.ps.sp.msg;

import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class UnsubMessage extends Message{

	private String key;
	
	public UnsubMessage()
	{
		super.setCmd(MessageTypes.UNSUB);
		super.setSeq(Message.SEQ.incrementAndGet());
	}
	
	public UnsubMessage(String key)
	{
		this.key = key;
		super.setCmd(MessageTypes.UNSUB);
		super.setSeq(Message.SEQ.incrementAndGet());
	}

	@Override
	public void encodeBody(ByteBuf buf) {
		if(key != null)
		{
			int length = key.length();
			buf.writeInt(length);
			buf.writeCharSequence(key, Charset.defaultCharset());
		}
		else 
		{
			buf.writeInt(0);
		}
	}

	@Override
	public void decodeBody(ByteBuf buf) {
		int length = buf.readInt();
		if(length > 0)
		{
			key = (String) buf.readCharSequence(length, Charset.defaultCharset());
		}
	}
}
