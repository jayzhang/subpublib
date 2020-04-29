package com.qxwz.ps.sp.msg;

import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class SubMessage extends Message{

	private String subscriberName;
	
	private String key;
	
	public SubMessage()
	{
		super.setCmd(MessageTypes.SUB);
		super.setSeq(Message.SEQ.incrementAndGet());
	}
	
	public SubMessage(String key)
	{
		this.key = key;
		super.setCmd(MessageTypes.SUB);
		super.setSeq(Message.SEQ.incrementAndGet());
	}

	@Override
	public void encodeBody(ByteBuf buf) {
		
		if(subscriberName != null)
		{
			int length = subscriberName.length();
			buf.writeInt(length);
			buf.writeCharSequence(subscriberName, Charset.defaultCharset());
		}
		else 
		{
			buf.writeInt(0);
		}
		
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
			subscriberName = (String) buf.readCharSequence(length, Charset.defaultCharset());
		}
		length = buf.readInt();
		if(length > 0)
		{
			key = (String) buf.readCharSequence(length, Charset.defaultCharset());
		}
	}
}
