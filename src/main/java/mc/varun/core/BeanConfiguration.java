package mc.varun.core;

import java.util.Collections;
import java.util.Properties;
import mc.varun.core.api.Messenger;
import mc.varun.core.util.EnvelopeSerdes;
import mc.varun.proto.EnvelopeOuterClass.Envelope;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
class BeanConfiguration implements ApplicationContextAware {

	private static final Logger LOGGER = LoggerFactory.getLogger(BeanConfiguration.class);

	private final EnvelopeSerdes envelopeSerdes;

	private final Environment environment;

	private ApplicationContext applicationContext;

	@Autowired
	BeanConfiguration(Environment environment, EnvelopeSerdes envelopeSerdes) {
		LOGGER.trace("BeanConfiguration::constructor({}, {})", environment, envelopeSerdes);
		this.envelopeSerdes = envelopeSerdes;
		this.environment = environment;
	}

	@Bean
	AdminClient adminClient() {
		LOGGER.trace("BeanConfiguration::adminClient()");
		Properties properties = new Properties();
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("kafka.bootstrapServers"));
		return AdminClient.create(properties);
	}

	@Bean
	Messenger messageBus() {
		LOGGER.trace("BeanConfiguration::messageBus()");
		String messageBusTopic = environment.getProperty("kafka.messageBus.topic");
		int messageBusPartitions = Integer.parseInt(environment.getProperty("kafka.messageBus.partitions"));
		short messageBusReplicationFactor = Short.parseShort(environment.getProperty("kafka.messageBus.replicationFactor"));
		// TODO: capture and handle result
		adminClient().createTopics(Collections.singleton(new NewTopic(messageBusTopic, messageBusPartitions, messageBusReplicationFactor)));
		return applicationContext.getBean(MessengerImpl.class, messageBusTopic);
	}

	@Bean
	KafkaProducer<String, Envelope> messageProducer() {
		LOGGER.trace("BeanConfiguration::messageProducer()");
		Properties configuration = new Properties();
		configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("kafka.bootstrapServers"));
		return new KafkaProducer<>(configuration, new StringSerializer(), envelopeSerdes.serializer());
	}

	@Bean
	Properties streamConfiguration() {
		LOGGER.trace("BeanConfiguration::streamConfiguration()");
		Properties streamConfiguration = new Properties();
		streamConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, environment.getProperty("application.id"));
		streamConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("kafka.bootstrapServers"));
		return streamConfiguration;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		LOGGER.trace("BeanConfiguration::setApplicationContext({})", applicationContext);
		this.applicationContext = applicationContext;
	}
}
