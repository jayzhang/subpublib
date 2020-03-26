package com.qxwz.ps.sp;

import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;

public interface ISubHandler {

	void handleSubMessage(SubMessage msg);
	
	void handleUnsubMessage(UnsubMessage msg);
}
