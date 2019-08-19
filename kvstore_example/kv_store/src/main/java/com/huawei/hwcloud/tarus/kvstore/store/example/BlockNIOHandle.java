package com.huawei.hwcloud.tarus.kvstore.store.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class BlockNIOHandle extends Thread{

	private static final Logger log = LoggerFactory.getLogger(BlockNIOHandle.class);

	private static final int timeout = 60 * 1000;

	private static final int buffsize = 2048;

	private int headlen = Integer.BYTES;

	private RpcProcess rpcProcess = null;

	private Socket socket = null;
	
	public BlockNIOHandle(Socket socket, RpcProcess rpcProcess){
		this.socket = socket;
		this.rpcProcess = rpcProcess;
	}

	public void run(){

		ByteArrayOutputStream baosHead = null;
		ByteArrayOutputStream baos = null;

		ByteBuffer headbuf = null;

		ByteBuffer resbuf = null;

		try{
			headbuf = ByteBuffer.allocate(headlen);
			headbuf.clear();

			socket.setSoTimeout(timeout);
			InputStream in = socket.getInputStream();
			BufferedInputStream bis = new BufferedInputStream(in);

			baosHead = new ByteArrayOutputStream();
			readPacket(bis, baosHead, Integer.BYTES);

			byte[] headMessage = baosHead.toByteArray();
			headbuf.put(headMessage);
			headbuf.flip();
			int head = headbuf.getInt();
			baos = new ByteArrayOutputStream();
			readPacket(bis, baos, head);

			byte[] req = baos.toByteArray();

			log.info("request message head=[{}], len=[{}]", head, req.length);

			//response message
			//rpc process
			byte[] res = rpcProcess.process(req);

			resbuf = ByteBuffer.allocate(headlen + res.length);
			resbuf.putInt(res.length).put(res);

			OutputStream out = socket.getOutputStream();
			BufferedOutputStream bos = new BufferedOutputStream(out);
			writeMessage(bos, resbuf.array());
		}catch(Exception e){
			log.warn("socket handle error!", e);
		}finally{
			if(socket != null){
				try {
					socket.close();
				} catch (IOException e) {
					log.warn("close error!", e);
				}
			}
		}
	}

	public static void readPacket(BufferedInputStream bis, ByteArrayOutputStream baos, int len) throws IOException {

		int index = 0;

		int numberRead = 0;
		int tmpLen = len;

		byte[] buff = null;

		for (int i = 0; i < 20000; i++) {

			if (tmpLen < buffsize) {

				buff = new byte[tmpLen];
				numberRead = bis.read(buff, 0, tmpLen);
			} else {

				buff = new byte[buffsize];
				numberRead = bis.read(buff, 0, buffsize);
			}

			if (numberRead < 0) {

				break;
			} else if (numberRead < tmpLen) {

				index += numberRead;
				tmpLen = tmpLen - numberRead;

				byte[] tmpByte = new byte[numberRead];
				System.arraycopy(buff, 0, tmpByte, 0, numberRead);
				baos.write(tmpByte);
			} else if (numberRead == tmpLen) {
				index += numberRead;
				tmpLen = tmpLen - numberRead;
				baos.write(buff);
			} else {
				break;
			}

			if (tmpLen <= 0) {

				break;
			}
		}
	}

	public static void writeMessage(BufferedOutputStream bos, byte[] message) throws IOException {

		int len = message.length;
		int count = 0;
		int tmpLen = len;

		while (true) {
			if (tmpLen <= buffsize) {
				bos.write(message, count, tmpLen);
				bos.flush();
				count = count + tmpLen;
				tmpLen = 0;
			} else {
				bos.write(message, count, buffsize);
				bos.flush();
				count = count + buffsize;
				tmpLen = tmpLen - buffsize;
			}
			if (tmpLen == 0) {
				break;
			}
		}
	}
	
//	public void run(){
//
//		ByteBuffer headbuf = null;
//		ByteBuffer databuf = null;
//
//		ByteBuffer resbuf = null;
//
//		try{
//			headbuf = ByteBuffer.allocate(headlen);
//			headbuf.clear();
//			//recieve request
//			//recieve header
//			cycleReadMessage(client, headbuf, 5000);
//
//			int head = headbuf.getInt();
//
//			databuf = ByteBuffer.allocate(head);
//			databuf.clear();
//			cycleReadMessage(client, databuf, 5000);
//
//			byte[] req = databuf.array();
//
//			log.info("request message head=[{}], len=[{}], req is=[{}]", head, req.length, BufferUtil.bytesToString(req));
//
//			//response message
//			//rpc process
//			byte[] res = rpcProcess.process(req);
//
//			resbuf = ByteBuffer.allocate(headlen + res.length);
//			resbuf.putInt(res.length).put(res);
//
//			//此处不需要反转，只有当在一系列通道读取或放置 操作之后，调用flip反转为一系列通道写入或相对获取 操作做好准备。
//			//此处buf为根据byte数组新建
//			//reqbuf.flip();
//			while(resbuf.hasRemaining()){
//				client.write(resbuf);
//			}
//		}catch(Exception e){
//			log.warn("socket handle error!", e);
//		}finally{
//			if(client != null){
//				try {
//					client.close();
//				} catch (IOException e) {
//					log.warn("close error!", e);
//				}
//			}
//		}
//
//	}
	
//	/**
//	 *
//	 * @param bis
//	 * @param baos
//	 * @param bufferCapacity
//	 * @throws IOException
//	 */
//	public static void cycleReadMessage(SocketChannel sc,
//			ByteBuffer buf,	int socketReaderCircleTimes) throws IOException {
//
//		// 每次读取的数据的值
//		int numberRead = 0;
//		int tmpLen = buf.capacity();
//		log.info("cycleReadMessage, len is=[{}]", tmpLen);
//		// 循环从输入流中读取数据
////		for (int i = 0; i < socketReaderCircleTimes; i++) {
//		while(true){
//			numberRead = sc.read(buf);
//			if (numberRead < 0) {
//
//				break;
//			} else if (numberRead <= tmpLen) {
//				tmpLen = tmpLen - numberRead;
//			} else {
//				break;
//			}
//
//			if (tmpLen <= 0) {
//
//				break;
//			}
//		}
//	}
}
