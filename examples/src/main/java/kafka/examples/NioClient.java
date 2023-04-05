package kafka.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * @author huangran <huangran@kuaishou.com>
 * Created on 2023-03-22
 */
public class NioClient {

    private final ByteBuffer sendBuffer = ByteBuffer.allocate(1024);
    private final ByteBuffer receiveBuffer = ByteBuffer.allocate(4);
    private final Selector selector;

    public NioClient() throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(new InetSocketAddress(InetAddress.getLocalHost(), 8888));
        System.out.println("与服务器的连接建立成功");
        selector = Selector.open();
        // NIO 是水平触发模式，所有注册了 OP_ACCEPT 事件后，只要 OS 写缓冲区不满，就会一直发出写信号通知
        socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        System.out.println(socketChannel);
    }

    public static void main(String[] args) throws Exception {
        final NioClient client = new NioClient();
//        Thread receiver = new Thread(client::receiveFromUser);
//
//        receiver.start();
        client.talk();
    }

    private void talk() throws IOException {
        while (selector.select() > 0) {

            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();
                System.out.println("就绪key:" + key);
                it.remove();

                if (key.isReadable()) {
                    System.out.println("client isReadable......");
                    receive(key);
                }
                // 实际上只要注册了关心写操作，这个操作就一直被激活
                if (key.isWritable()) {
                    send(key);
                }
            }
        }
    }

    private void send(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        synchronized (sendBuffer) {
            sendBuffer.flip(); //设置写
            while (sendBuffer.hasRemaining()) {
                System.out.println("发送数据：" + new String(sendBuffer.array()));
                socketChannel.write(sendBuffer);
            }
            sendBuffer.compact();
        }
    }

    private void receive(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        socketChannel.read(receiveBuffer);
        receiveBuffer.flip();
        String receiveData = StandardCharsets.UTF_8.decode(receiveBuffer).toString();

        System.out.println("receive server message:" + receiveData);
        receiveBuffer.clear();
    }

    private void receiveFromUser() {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        try {
            String msg;
            while ((msg = bufferedReader.readLine()) != null) {
                synchronized (sendBuffer) {
                    sendBuffer.put((msg + "\r\n").getBytes());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
