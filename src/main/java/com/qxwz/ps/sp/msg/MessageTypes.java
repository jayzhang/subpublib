package com.qxwz.ps.sp.msg;

public interface MessageTypes {
	public final short SUB = 0x00; //订阅
	public final short UNSUB = 0x01; //反订阅
	public final short PUB = 0x02;  //数据发布
	public final short HB = 0x03; //心跳
}
