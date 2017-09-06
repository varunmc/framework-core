package mc.varun.core.util;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import mc.varun.proto.EnvelopeOuterClass.Envelope;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
final class EnvelopeDeserializer implements Deserializer<Envelope> {

	private static final Logger LOGGER = LoggerFactory.getLogger(EnvelopeDeserializer.class);

	@Override
	public void close() {
		LOGGER.trace("EnvelopeDeserializer::close()");
		// NOOP
	}

	@Override
	public void configure(Map<String, ?> configuration, boolean isKey) {
		LOGGER.trace("EnvelopeDeserializer::configure({}, {})", configuration, isKey);
		// NOOP
	}

	@Override
	public Envelope deserialize(String topic, byte[] bytes) {
		LOGGER.trace("EnvelopeDeserializer::deserialize({}, {})", topic, bytes);
		try {
			return Envelope.parseFrom(bytes);
		} catch (InvalidProtocolBufferException e) {
			LOGGER.error("Error parsing envelope", e);
		}
		return null;
	}
}
