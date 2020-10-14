/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;

public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // Master维护的连接数。（Slave的个数）
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    // 具体连接信息
    private final List<HAConnection> connectionList = new LinkedList<>();

    // 服务端接收连接线程实现类
    private final AcceptSocketService acceptSocketService;

    // broker存储实现类
    private final DefaultMessageStore defaultMessageStore;

    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    // 该Master所有Slave中同步最大的偏移量。
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    // 判断主从同步复制是否完成
    private final GroupTransferService groupTransferService;

    // HA客户端实现，Slave端网络的实现类。
    private final HAClient haClient;

    // RocketMQ HA机制大体可以分为如下三个部分。
    //
    // Master启动并监听Slave的连接请求。
    // Slave启动，与Master建立链接。
    // Slave发送待拉取偏移量待Master返回数据，持续该过程。
    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    // 该方法是在Master收到从服务器的拉取请求，拉取请求是slave下一次待拉取的消息偏移量，也可以认为是Slave的拉取偏移量确认信息，
    // 如果该信息大于push2SlaveMaxOffset，则更新push2SlaveMaxOffset，
    // 然后唤醒GroupTransferService线程，各消息发送者线程再判断push2SlaveMaxOffset与期望的偏移量进行对比。
    public void notifyTransferSome(final long offset) {
        // 如果slaveAckOffset（从broker的ack偏移）大于HAService.push2SlaveMaxOffset的值则更新push2SlaveMaxOffset的值，
        // 并通知调用GroupTransferService.notifyTransferSome方法唤醒GroupTransferService服务线程。
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    // 在主备Broker启动的时候，启动HAService服务，启动了如下服务：
    public void start() throws Exception {
        // 1）HAService.AcceptSocketService：
        // 该服务是主用Broker使用，该服务主要监听新的Socket连接，若有新的连接到来，则创建HAConnection对象，
        // 在该对象中创建了HAConnection.WriteSocketService和HAConnection.ReadSocketService线程服务，
        // 对新来的socket连接分别进行读和写的监听。
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        // 2）HAService.GroupTransferService：
        // 该服务是对同步进度进行监听，若达到应用层的写入偏移量，则通知应用层该同步已经完成。
        // 在调用CommitLog.putMessage方法写入消息内容时，根据主用broker的配置来决定是否利用该服务进行同步等待数据同步的结果。
        this.groupTransferService.start();
        // 3）HAService.HAClient：
        // 该服务是备用Broker使用，在备用Broker启动之后与主用Broker建立socket连接，
        // 然后将备用Broker的commitlog文件的最大数据位置每隔5秒给主用Broker发送一次；
        // 并监听主用Broker的返回消息。
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        private final SocketAddress socketAddressListen;
        private ServerSocketChannel serverSocketChannel;
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         * 开始监听slave的连接
         *
         * 在启动该模块时，注册监听Socket的OP_READ操作，
         * 若有该操作达到，即有备用Broker请求连接到该主用Broker上时，
         * 利用该ScoketChannel初始化HAConnection对象，并调用该对象的start方法，
         * 在该方法中启动该对象的ReadSocketService和WriteSocketService服务线程，
         * 然后将该HAConnection对象存入HAService.connectionList:List<HAConnection>变量中，
         * 该变量是存储客户端连接，用于管理连接的删除和销毁。
         *
         * ReadSocketService服务线程用于读取备用Broker的请求，WriteSocketService服务线程用于向备用Broker写入数据。
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 创建HAConnection对象，并启动ReadSocketService和WriteSocketService服务线程
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     *
     * GroupTransferService同步主从同步阻塞实现，
     * 如果是同步主从模式，消息发送者将消息刷写到磁盘后，需要继续等待新数据被传输到从服务器，
     * 从服务器数据的复制是在另外一个线程HAConnection中去拉取，所以消息发送者在这里需要等待数据传输的结果，
     * GroupTransferService就是实现该功能，该类的整体结构与同步刷盘实现类(CommitLog$GroupCommitService)类似
     *
     * 该服务是对同步进度进行监听，若达到应用层的写入偏移量，则通知应用层该同步已经完成。
     * 在调用CommitLog.putMessage方法写入消息内容时，根据主用broker的配置来决定是否利用该服务进行同步等待数据同步的结果。
     *
     * 应用层将请求放入requestsWrite队列中，
     * 当该服务线程被唤醒时，首先将requestsWrite队列的请求与requestsRead队列的请求交换，
     * 而requestsRead队列一般都是空的，也就是将requestsWrite队列的内容赋值到requestsRead队列之后再清空；
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            this.wakeup();
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    // 然后遍历requestsRead队列的每个GroupCommitRequest对象，用该对象的NextOffset值与push2SlaveMaxOffset比较，
                    // 若push2SlaveMaxOffset大于了该对象的NextOffset值则置GroupCommitRequest请求对象的flushOK变量为true，
                    // 在调用处对该变量有监听。
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        // 判断主从同步是否完成的依据是：
                        // 所有Slave中已成功复制的最大偏移量是否大于等于消息生产者发送消息后消息服务端返回下一条消息的起始偏移量，
                        // 如果是则表示主从同步复制已经完成，唤醒消息发送线程，
                        // 否则等待1s,再次判断，每一个任务在一批任务中循环判断5次。
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                            + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        // 设置同步请求的状态
                        req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }

                    // 情况requestsRead列表
                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 结束等待（被唤醒）的时候调用onWaitEnd方法中的swapRequests()交换requestsWrite列表和requestsRead列表
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * @Description:
     *
     * 在备用Broker启动的时候，启动了HAService.HAClient线程服务，
     * 该线程有两个作用，第一，每隔5秒发送一次心跳消息；第二，接受主用Broker的返回数据，然后进行后续处理。
     *
     * @date 2020/10/13 下午4:07
     * @param
     * @return
     */
    class HAClient extends ServiceThread {
        // Socket读缓存区大小
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        // master地址
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        // Slave向Master发起主从同步的拉取偏移量，固定8个字节
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        private SocketChannel socketChannel;
        private Selector selector;
        private long lastWriteTimestamp = System.currentTimeMillis();

        // 反馈Slave当前的复制进度，commitlog文件最大偏移量
        private long currentReportedOffset = 0;
        // 本次已处理读缓存区的指针
        private int dispatchPosition = 0;
        // 读缓存区，大小为4M
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // 读缓存区备份，用于BufferRead进行交换
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        private boolean isTimeToReportOffset() {
            // 判断是否需要向Master汇报已拉取消息偏移量。其依据为每次拉取间隔必须大于haSendHeartbeatInterval，默认5s
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        private boolean reportSlaveMaxOffset(final long maxOffset) {
            // 如果需要向Master反馈当前拉取偏移量，则向Master发送一个8字节的请求，请求包中包含的数据为当前Broker消息文件的最大偏移量
            //
            // 这里RocketMQ的作者改成了一个基本的ByteBuffer操作示例：
            // 首先分别将ByteBuffer的position、limit设置为0与ByteBuffer的总长度，
            // 然后将偏移量写入到ByteBuffer中，
            // 然后需要将ByteBuffer的当前状态从写状态转换为读状态，以便将数据传入通道中。
            //
            // RocketMQ作者采用的方法是手段设置position指针为0，limit为ByteBuffer容易，
            // 其实这里可以通过调用ByteBuffer的flip()方法达到同样的目的，
            //
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            // 将一个ByteBuffer写入到通道，通常使用循环写入，
            // 判断一个ByteBuffer是否全部写入到通道的一个方法是调用ByteBuffer#hasRemaining()方法。
            // 如果返回false,表示在进行网络读写时发生了IO异常，此时会关闭与Master的连接。
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            // 1.A）检查byteBufferRead变量中的二进制数据是否解析完了（reamin=ReadMaxBufferSize-dispatchPostion）
            // 如果remain>0表示没有解析完，则将剩下的数据复制到HAClient.byteBufferBackup变量中；
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            // 1.B）将byteBufferRead和byteBufferBackup的数据进行交换；
            this.swapByteBuffer();

            // 1.C）重新初始化byteBufferRead变量的position等于remain，即表示byteBufferRead中写入到了位置position；
            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            // 从SocketChannel读取主用Broker返回的数据，一直循环的读取并解析数据
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        // 调用HAClient.dispatchReadRequest()方法对数据解析和处理，
                        // 在dispatchReadRequest方法中循环的读取byteBufferRead变量中的数据
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) { // 若为空将重复读取3次后仍然没有数据则跳出该循环。
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        // 该方法主要从byteBufferRead中解析一条一条的消息，然后存储到commitlog文件并转发到消息消费队列与索引文件中
        private boolean dispatchReadRequest() {
            // msgHeaderSize,头部长度，大小为12个字节，包括消息的物理偏移量与消息的长度，长度字节必须首先探测，
            // 否则无法判断byteBufferRead缓存区中是否包含一条完整的消息。readSocketPos：记录当前byteBufferRead的当前指针
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            // HAClient.dispatchPosition变量来标记从byteBufferRead变量读取数据的位置,初始化值为0；
            // byteBufferRead变量的position值表示从SocketChannel中收到的数据的最后位置
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                // 先探测byteBufferRead缓冲区中是否包含一条消息的头部，
                // 如果包含头部，则读取物理偏移量与消息长度，然后再探测是否包含一条完整的消息，
                // 如果不包含，则需要将byteBufferRead中的数据备份，以便更多数据到达再处理
                //
                // 比较position减dispatchPosition的值大于12（消息头部长度为12个字节）
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                // 大于12个字节表示有心跳消息从主用Broker发送过来，进行如下处理
                if (diff >= msgHeaderSize) {
                    // A）在byteBufferRead中从dispatchPosition位置开始读取数据，初始化状态下dispatchPosition等于0；
                    // 读取8个字节的数据即为主用Broker的同步的起始物理偏移量masterPhyOffset，再后4字节为数据的大小bodySize；
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    // B）从备用Broker中获取最大的物理偏移量，
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    // 如果byteBufferRead中包含一则消息头部，则读取物理偏移量与消息的长度，然后获取Slave当前消息文件的最大物理偏移量，
                    // 如果slave的最大物理偏移量与master给的偏移量不相等，则返回false，
                    // 从后面的处理逻辑来看，返回false,将会关闭与master的连接，在Slave本次周期内将不会再参与主从同步了
                    //
                    // 如果与主用Broker传来的起始物理偏移量masterPhyOffset不相等，则直接返回继续执行；
                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    // dispatchPosition：表示byteBufferRead中已转发的指针。
                    // 设置byteBufferRead的position指针为dispatchPosition+msgHeaderSize,
                    // 然后读取bodySize个字节内容到byte[]字节数组中，
                    // 并调用DefaultMessageStore#appendToCommitLog方法将消息内容追加到消息内存映射文件中，
                    // 然后唤醒ReputMessageService实时将消息转发给消息消费队列与索引文件，更新dispatchPosition，
                    // 并向服务端及时反馈当前已存储进度。将所读消息存入内存映射文件后重新向服务端发送slave最新的偏移量
                    //
                    // C）若position-dispatchPosition的值大于消息头部长度12字节加上bodySize之和；
                    // 则说明有数据同步，则继续在byteBufferRead中以position+dispatchPosition开始位置读取bodySize大小的数据；
                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);

                        // D）调用DefaultMessageStore.appendToCommitLog(long startOffset, byte[] data)方法进行数据的写入；
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        // E）将byteBufferRead变量的position值重置为readSocketPos；dispatchPosition值累计12+bodySize；
                        this.byteBufferRead.position(readSocketPos);
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        // F）检查当前备用Broker的最大物理偏移量是否大于了上次向主用Broker报告时的最大物理偏移量（HAClient.currentReportedOffset），
                        // 若大于则更新HAClient.currentReportedOffset的值，并将最新的物理偏移量向主用Broker报告。
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        // G）继续读取byteBufferRead变量中的数据
                        continue;
                    }
                }

                // 小于12个字节，并且byteBufferRead变量中没有可写空间（this.position>=this.limit）,
                // 则调用HAClient.reallocateByteBuffer()方法进行ByteBuffer的整理
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            // 比较当前slave broker的最大物理偏移量是否大于上次向master broker报告时的最大物理偏移量（HAClient.currentReportedOffset），
            // 如果大于则更新HAClient.currentReportedOffset的值，并将最新的物理偏移量向master broker报告。
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                // 获取主用Broker的地址
                String addr = this.masterAddress.get();
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            // 与主Broker建立Socket链接，并在该链接上注册OP_READ操作，即监听master broker返回的消息
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                // 获取备用Broker本地的最大写入位置即最大物理偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                // 更新最后写入时间戳
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 1、从HAService.HAClient.masterAddress变量中获取主用Broker的地址，
                    // 在备用Broker向Name Server注册时会返回主用Broker的地址；
                    // 若有主用Broker地址则与主用Broker建立Socket链接，并在该链接上注册OP_READ操作，即监听主用Broker返回的消息；
                    // 调用DefaultMessageStore.getMaxPhyOffset()方法获取备用Broker本地的最大写入位置即最大物理偏移量，
                    // 然后赋值给HAClient.currentReportedOffset变量；
                    // 更新最后写入时间戳lastWriteTimestamp；
                    if (this.connectMaster()) {

                        // 2、检查上次写入时间戳lastWriteTimestamp距离现在是否已经过了5秒，
                        // 即每隔5秒向主用Broker进行一次物理偏移量报告（HAClient.currentReportedOffset）；
                        // 若超过了5秒，则备用Broker向主用Broker报告备用Broker的当前最大物理偏移量的值，
                        // 该消息只有8个字节，即为物理偏移量的值；
                        if (this.isTimeToReportOffset()) {
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 3、该服务线程等待1秒钟，
                        // 然后从SocketChannel读取主用Broker返回的数据，一直循环的读取并解析数据，
                        // 直到HAClient.byteBufferRead:ByteBuffer中无可写的空间为止，
                        // 当ByteBuffer中的this.position<this.limit时表示有可写空间,
                        // 该byteBufferRead变量是在初始化时HAClient是创建的，初始化空间为ReadMaxBufferSize=4G，
                        // 若为空将重复读取3次后仍然没有数据则跳出该循环。
                        //
                        // 若读取到数据，则首先更新HAClient.lastWriteTimestamp变量；
                        // 然后调用HAClient.dispatchReadRequest()方法对数据解析和处理，
                        // 在dispatchReadRequest方法中循环的读取byteBufferRead变量中的数据
                        this.selector.select(1000);

                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        // 4、在第3步返回之后，检查当前备用Broker的最大物理偏移量是否大于了上次向主用Broker报告时的最大物理偏移量（HAClient.currentReportedOffset），
                        // 若大于则更新HAClient.currentReportedOffset的值，并将最新的物理偏移量向主用Broker报告。
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        // 5、检查最后写入时间戳lastWriteTimestamp距离当前的时间，
                        // 若大于了5秒，表示这期间未收到过主用Broker的消息，则关闭与主用Broker的连接
                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
