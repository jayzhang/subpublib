package com.qxwz.ps.sp.msg;

import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class PubMessage extends Message{

	private String key;
	private byte[] data;
	
	public PubMessage()
	{
		super.setCmd(MessageTypes.PUB);
		super.setSeq(Message.SEQ.incrementAndGet());
	}
	
	public PubMessage(String key, byte[] data)
	{
		this.key = key;
		this.data = data;
		super.setCmd(MessageTypes.PUB);
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
		
		if(data != null)
		{
			int length = data.length;
			buf.writeInt(length);
			buf.writeBytes(data);
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
		
		length = buf.readInt();
		if(length > 0)
		{
			data = new byte[length];
			buf.readBytes(data);
		}
	}
}
