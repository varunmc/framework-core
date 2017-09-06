package mc.varun.core.util;

import java.util.Map;
import mc.varun.proto.EnvelopeOuterClass.Envelope;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
final class EnvelopeSerializer implements Serializer<Envelope> {

	private static final Logger LOGGER = LoggerFactory.getLogger(EnvelopeSerializer.class);

	@Override
	public void close() {
		LOGGER.trace("EnvelopeSerializer::close()");
		// NOOP
	}

	@Override
	public void configure(Map<String, ?> configuration, boolean isKey) {
		LOGGER.trace("EnvelopeSerializer::configure({}, {})", configuration, isKey);
		// NOOP
	}

	@Override
	public byte[] serialize(String topic, Envelope envelope) {
		LOGGER.trace("EnvelopeSerializer::serialize({}, {})", topic, envelope);
		return envelope.toByteArray();
	}
}
