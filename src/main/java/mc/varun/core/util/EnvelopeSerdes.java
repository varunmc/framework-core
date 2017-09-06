package mc.varun.core.util;

import java.util.Map;
import mc.varun.proto.EnvelopeOuterClass.Envelope;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public final class EnvelopeSerdes implements Serde<Envelope> {

	private static final Logger LOGGER = LoggerFactory.getLogger(EnvelopeSerdes.class);

	private final EnvelopeDeserializer deserializer;

	private final EnvelopeSerializer serializer;

	@Autowired
	private EnvelopeSerdes(EnvelopeDeserializer deserializer, EnvelopeSerializer serializer) {
		LOGGER.trace("EnvelopeSerdes::constructor({}, {})", deserializer, serializer);
		this.deserializer = deserializer;
		this.serializer = serializer;
	}

	@Override
	public void close() {
		LOGGER.trace("EnvelopeSerdes::close()");
		// NOOP
	}

	@Override
	public void configure(Map<String, ?> configuration, boolean isKey) {
		LOGGER.trace("EnvelopeSerdes::configure({}, {})", configuration, isKey);
		// NOOP
	}

	@Override
	public Deserializer<Envelope> deserializer() {
		LOGGER.trace("EnvelopeSerdes::deserializer()");
		return deserializer;
	}

	@Override
	public Serializer<Envelope> serializer() {
		LOGGER.trace("EnvelopeSerdes::serializer()");
		return serializer;
	}
}
