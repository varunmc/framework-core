package mc.varun.core;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import mc.varun.core.api.MessageHandler;
import mc.varun.core.api.Messenger;
import mc.varun.core.util.EnvelopeSerdes;
import mc.varun.proto.EnvelopeOuterClass.Envelope;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
final class MessengerImpl implements Messenger {

	private static final Logger LOGGER = LoggerFactory.getLogger(MessengerImpl.class);

	private final AtomicBoolean isStarted;

	private final KStreamBuilder streamBuilder;

	private final String topic;

	private String applicationId;

	private EnvelopeSerdes envelopeSerdes;

	private KafkaProducer<String, Envelope> messageProducer;

	private KStream<String, Envelope> messageStream;

	private Properties streamConfiguration;

	private MessengerImpl(String topic) {
		LOGGER.trace("MessengerImpl::constructor({})", topic);
		this.topic = topic;
		isStarted = new AtomicBoolean(false);
		streamBuilder = new KStreamBuilder();
	}

	private <M extends Message> Envelope createEnvelope(M message, Envelope original) {
		LOGGER.trace("MessengerImpl({})::createEnvelope({}, {})", topic, message, original);
		String correlationId = UUID.randomUUID().toString();
		String requesterId = applicationId;
		if (original != null) {
			if (original.getCorrelationId() != null) {
				correlationId = original.getCorrelationId();
			}
			if (original.getRequesterId() != null) {
				requesterId = original.getRequesterId();
			}
		}
		return Envelope.newBuilder()
			.setId(UUID.randomUUID().toString())
			.setCorrelationId(correlationId)
			.setApplicationId(applicationId)
			.setRequesterId(requesterId)
			.setContents(Any.pack(message))
			.build();
	}

	@PostConstruct
	private void postConstruct() {
		LOGGER.trace("MessengerImpl({})::postConstruct()", topic);
		// TODO: determine why this method is called multiple times and remove guard
		if (messageStream != null) {
			return;
		}
		messageStream = streamBuilder.stream(Serdes.String(), envelopeSerdes, topic);
		messageStream.foreach((correlationId, envelope) -> LOGGER.debug("Received envelope on {}:{}", topic, envelope));
	}

	@Override
	public <M extends Message> void register(MessageHandler<M> handler, Class<M> messageClass) {
		register(handler, messageClass, true);
	}

	@Override
	public <M extends Message> void register(MessageHandler<M> handler, Class<M> messageClass, boolean allMessages) {
		LOGGER.trace("MessengerImpl({})::register({}, {}, {})", topic, handler, messageClass, allMessages);
		handler.handle(messageStream
			.filter((correlationId, envelope) -> allMessages || applicationId.equals(envelope.getRequesterId()))
			.map((correlationId, envelope) -> new KeyValue<>(envelope, unpack(envelope)))
			.filter((envelope, message) -> message != null && (message.getClass().equals(messageClass) || messageClass == null))
			.map((envelope, message) -> new KeyValue<>(envelope, messageClass.cast(message))))
			.filter((envelope, response) -> response != null)
			.map((envelope, response) -> new KeyValue<>(envelope.getCorrelationId(), createEnvelope(response, envelope)))
			.to(Serdes.String(), envelopeSerdes, topic);
	}

	@Override
	public <M extends Message> void send(M message) {
		LOGGER.trace("MessengerImpl({})::send({})", topic, message);
		Envelope envelope = createEnvelope(message, null);
		LOGGER.debug("Sending envelope on {}:{}", topic, envelope);
		messageProducer.send(new ProducerRecord<>(topic, envelope.getCorrelationId(), envelope));
	}

	@Override
	public void start() {
		LOGGER.trace("MessengerImpl({})::start()", topic);
		if (isStarted.compareAndSet(false, true)) {
			KafkaStreams streams = new KafkaStreams(streamBuilder, streamConfiguration);
			Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
			streams.start();
		} else {
			LOGGER.warn("Already started for {}", topic);
		}
	}

	private Message unpack(Envelope envelope) {
		LOGGER.trace("MessengerImpl({})::unpack({})", topic, envelope);
		try {
			Any envelopeContents = envelope.getContents();
			String messageTypeUrl = envelopeContents.getTypeUrl();
			String[] messageTypeTokens = messageTypeUrl.split("\\.");
			String messageType = messageTypeTokens[messageTypeTokens.length - 1];
			messageTypeUrl += "OuterClass$" + messageType;
			String messageClassName = messageTypeUrl.split("/")[1];
			Class<? extends Message> messageClass = (Class<? extends Message>) Class.forName(messageClassName);
			return envelopeContents.unpack(messageClass);
		} catch (ClassNotFoundException | InvalidProtocolBufferException e) {
			LOGGER.error("Error unpacking envelope", e);
			return null;
		}
	}

	@Autowired
	private void setEnvelopeSerdes(EnvelopeSerdes envelopeSerdes) {
		LOGGER.trace("MessengerImpl({})::setEnvelopeSerdes({})", topic, envelopeSerdes);
		this.envelopeSerdes = envelopeSerdes;
	}

	@Autowired
	public void setEnvironment(Environment environment) {
		LOGGER.trace("MessengerImpl({})::setEnvironment({})", topic, environment);
		applicationId = environment.getProperty("application.id");
	}

	@Autowired
	private void setMessageProducer(KafkaProducer<String, Envelope> messageProducer) {
		LOGGER.trace("MessengerImpl({})::setMessageProducer({})", topic, messageProducer);
		this.messageProducer = messageProducer;
	}

	@Autowired
	private void setStreamConfiguration(Properties streamConfiguration) {
		LOGGER.trace("MessengerImpl({})::setStreamConfiguration({})", topic, streamConfiguration);
		this.streamConfiguration = streamConfiguration;
	}
}
