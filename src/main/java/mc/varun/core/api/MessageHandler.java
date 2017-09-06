package mc.varun.core.api;

import com.google.protobuf.Message;
import mc.varun.proto.EnvelopeOuterClass.Envelope;
import org.apache.kafka.streams.kstream.KStream;

@FunctionalInterface
public interface MessageHandler<M extends Message> {

	KStream<Envelope, ? extends Message> handle(KStream<Envelope, M> stream);
}
