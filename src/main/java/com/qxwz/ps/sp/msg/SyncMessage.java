package com.qxwz.ps.sp.msg;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class SyncMessage extends Message{
  
	@Override
	public void encodeBody(ByteBuf buf) {
		 
	}

	@Override
	public void decodeBody(ByteBuf buf) {
		 
	}
}
