package mc.varun.core.api;

import com.google.protobuf.Message;

public interface Messenger {

	<M extends Message> void register(MessageHandler<M> handler, Class<M> messageClass);

	<M extends Message> void register(MessageHandler<M> handler, Class<M> messageClass, boolean allMessages);

	<M extends Message> void send(M message);

	void start();
}
