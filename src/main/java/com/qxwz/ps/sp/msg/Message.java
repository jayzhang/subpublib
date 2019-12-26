package com.qxwz.ps.sp.msg;

import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public abstract class Message {
	
	public static AtomicInteger SEQ = new AtomicInteger();
	
	private short cmd; //1
	private long seq;  //4
	
	public void encodeHead(ByteBuf buf)
	{
		buf.writeByte(cmd);
		buf.writeInt((int)seq);
	}
	public void decodeHead(ByteBuf buf)
	{
		cmd = buf.readUnsignedByte();
		seq = buf.readUnsignedInt();
	}
	abstract public void encodeBody(ByteBuf buf);
	abstract public void decodeBody(ByteBuf buf);
}
