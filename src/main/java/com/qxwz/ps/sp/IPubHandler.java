package com.qxwz.ps.sp;

import com.qxwz.ps.sp.msg.PubMessage;

public interface IPubHandler {

	void handlePubMessage(PubMessage msg);
}
