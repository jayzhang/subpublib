package com.qxwz.ps.sp;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qxwz.ps.sp.msg.PubMessage;
import com.qxwz.ps.sp.msg.SubMessage;
import com.qxwz.ps.sp.msg.UnsubMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
@Sharable
public class Subscriber extends ChannelInboundHandlerAdapter implements IZkChildListener, IZkDataListener{

    @Setter
	private int maxReconnCount = 5; //最大重连次数

	@Setter
	private int reconnInteval = 1000; //重连间隔

    private String zkBasePath;
	private ZkClient zkclient;
	private EventLoopGroup workerGroup;
	private Bootstrap clientbootstrap;
	private volatile Map<String, Channel> channelMap = new HashMap<>(); // remote channel address ----> channel
	private volatile Map<String, Set<String>> routerMap = new HashMap<>(); // remote channel address ----> key set
	private volatile Map<String, Channel> key2channel = new HashMap<>();  // key------>channel
	private ExecutorService executorService = Executors.newSingleThreadExecutor();

	@Setter
	private IPubHandler pubHandler;


    public Subscriber(ZkClient zkclient, String zkBasePath)
	{
		this.zkclient = zkclient;
		this.zkBasePath = zkBasePath;
	}


    public void init()
	{
		if(!zkclient.exists(zkBasePath))
		{
			zkclient.createPersistent(zkBasePath);
		}

		workerGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("Subscriber-Worker"));
		clientbootstrap = new Bootstrap();
		clientbootstrap.group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
        .handler(new SubscriberInitializer(this));
		zkclient.subscribeChildChanges(zkBasePath, this);
		List<String> children = zkclient.getChildren(zkBasePath);
		try {
			handleChildChange(zkBasePath, children);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	@Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception
	{
		log.info("handleChildChange, parentPath:{}, currentChilds:{}", parentPath, currentChilds);
		Set<String> toDel = new HashSet<>();
		for(String addr: channelMap.keySet())
		{
			if(!currentChilds.contains(addr))
			{
				channelMap.get(addr).close();
				log.info("pub结点下线，关闭连接:{}", addr);
				toDel.add(addr);
			}
		}
		for(String k: toDel)
		{
			channelMap.remove(k);
		}
		for(String child: currentChilds )
		{
			if(!channelMap.containsKey(child))
			{
				Channel channel = connect(child);
				if(channel != null)
				{
					String dataPath = parentPath + "/" + child;
					zkclient.subscribeDataChanges(dataPath, this);  //订阅数据变化
					Object data = zkclient.readData(dataPath, true);
					handleDataChange(dataPath, data); //初始化读取路由表
					log.info("新的pub上线，subscribeDataChanges, path:{}", parentPath + "/" + child);
				}
			}
		}
	}

    private Channel connect(String addr)
	{
		List<String> arr = Splitter.on(":").splitToList(addr);
		try {
			ChannelFuture future = clientbootstrap.connect(arr.get(0), Integer.parseInt(arr.get(1))).sync();
			log.info("连接成功:{}", future.channel());
			channelMap.put(addr, future.channel());
			checkFailedSubs();// 只要有新的连接成功，则检查是否有历史失败的订阅
			return future.channel();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

    private void checkFailedSubs()
	{
		for(String k: key2channel.keySet())
		{
			Channel channel = key2channel.get(k);
			if(channel == null || !channel.isOpen())
			{
				log.info("重新发起订阅:{}", k);
				this.subscribe(k);
			}
		}
	}


	@Override
	public void handleDataChange(String dataPath, Object data) throws Exception {
		log.info("handleDataChange, dataPath:{}, data:{}", dataPath, data);
		String addr = StringUtils.substringAfterLast(dataPath, "/");
		if(data != null)
		{
			String str = (String)data;
			List<String> keylist = Splitter.on(",").omitEmptyStrings().splitToList(str);
			Set<String> keys = Sets.newHashSet(keylist);
			routerMap.put(addr, keys);
		}
		else
		{
			routerMap.put(addr, new HashSet<>());
		}
	}


	@Override
	public void handleDataDeleted(String dataPath) throws Exception {
		log.info("handleDataDeleted, dataPath:{}", dataPath);
		String addr = StringUtils.substringAfterLast(dataPath, "/");
		routerMap.remove(addr);
	}

    public void subscribe(String key)
	{
		log.info("尝试订阅数据, key:{}", key);
		List<String> existingList = new ArrayList<>();
		int min = Integer.MAX_VALUE;
		String idlePub = null;

        int existingMin = Integer.MAX_VALUE;
		String existingIdlePub = null;

        List<String> addrs = Lists.newArrayList(routerMap.keySet());

        Collections.shuffle(addrs);

        for (String addr : addrs)
		{
			Set<String> keys = routerMap.get(addr);
			if(keys.contains(key))
			{
				existingList.add(addr);
				if(existingMin > keys.size())
				{
					existingMin = keys.size();
					existingIdlePub = addr;
				}
			}
			if(min > keys.size())
			{
				min = keys.size();
				idlePub = addr;
			}
		}
		Channel channel = null;
		if(existingList.size() == 0)  //没有现存订阅的结点，随机选一台pub
		{
			channel = channelMap.get(idlePub);
			log.info("key:{}未订阅过，挑选出最空闲的pub: {}, 负载:{}", key, idlePub, min);
		}
		else //有现存订阅的结点，在现存中随机选一台pub
		{
			channel = channelMap.get(existingIdlePub);
			log.info("key:{}已订阅过，挑选出最空闲的pub: {}, 负载:{}", key, existingIdlePub, existingMin);
		}
		if(channel != null)
		{
			SubMessage sub = new SubMessage(key);
			channel.writeAndFlush(sub);
			key2channel.put(key, channel);
			log.info("向{}订阅数据:{}", channel.remoteAddress(), key);
        } else
		{
			key2channel.put(key, null); //暂时无法找到pub
			log.error("没有pub可订阅!");
		}
	}

    public void close()
	{
		if(workerGroup != null)
		{
			workerGroup.shutdownGracefully();
		}
        /**
         * @author yao.lai
         * shutdown executorService
         */
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    private void transferKeys(Channel channel)
	{
		String addr = null;

        for (Entry<String, Channel> entry : channelMap.entrySet())
		{
			if(entry.getValue() == channel)
			{
				addr = entry.getKey();
				break;
			}
		}

        if (addr != null)
		{
			channelMap.remove(addr);
            routerMap.remove(addr);
        }

        Set<String> keys = new HashSet<>();
		for(Entry<String, Channel> entry: key2channel.entrySet())
		{
			if(entry.getValue() == channel)
			{
				keys.add(entry.getKey());
			}
		}
		if(keys.size() > 0)
		{
			log.info("故障转移的key:{}", keys);
			for(String key: keys)
			{
				this.subscribe(key);
			}
		}
	}

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        String remote = ctx.channel().remoteAddress().toString().substring(1);

        Set<String> setNullKeys = new HashSet<>();
		for(String k : key2channel.keySet())
		{
			if(ctx.channel() == key2channel.get(k) )
			{
				setNullKeys.add(k);
			}
		}

        executorService.execute(new Runnable() {

			@Override
			public void run() {
				Channel channel = null;
				for(int i = 0 ; i < maxReconnCount; ++ i)
				{
					log.warn("remote 关闭，重新连接:{}", remote);
					channel = connect(remote);
					if(channel != null) //重连成功，重新维护订阅关系
					{
						return ;
					}
					try {
						Thread.sleep(reconnInteval);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				//重连几次都失败
				transferKeys(ctx.channel());
			}
		});

//		transferKeys(ctx.channel());
    }

    public void unsubscribe(String key)
	{
		log.info("尝试取消订阅数据, key:{}", key);
		Channel channel = key2channel.get(key);
		if(channel != null)
		{
			UnsubMessage sub = new UnsubMessage(key);
			channel.writeAndFlush(sub);
			key2channel.remove(key);
			log.info("向{}取消订阅数据:{}", channel.remoteAddress(), key);
		}
	}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof PubMessage) {
            PubMessage pub = (PubMessage) msg;
            /**
             * @author yao.lai
             * remove msgNum statistic to avoid int parameter overflow
             */
            if (log.isDebugEnabled()) {
                log.debug("receive subscribe data,key:{}, remoteAddress:{}"
                        , pub.getKey(), ctx.channel().remoteAddress());

            }
			if(pubHandler != null)
			{
				pubHandler.handlePubMessage(pub);
			}
		}
    }
}
