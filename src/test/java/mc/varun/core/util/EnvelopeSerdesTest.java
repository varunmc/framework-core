package mc.varun.core.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import mc.varun.core.TestConfiguration;
import mc.varun.proto.EnvelopeOuterClass.Envelope;
import mc.varun.proto.TestMessageOuterClass.TestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Tests the serialization/deserialization of an envelope.
 */
@ContextConfiguration(classes = TestConfiguration.class)
public final class EnvelopeSerdesTest extends AbstractTestNGSpringContextTests {

	private static final String ID = "TEST_ID";

	private static final Logger LOGGER = LoggerFactory.getLogger(EnvelopeSerdesTest.class);

	private static final String TOPIC = "TEST_TOPIC";

	@Autowired
	private EnvelopeDeserializer deserializer;

	@Autowired
	private EnvelopeSerializer serializer;

	@Test
	public void testEmptyEnvelopeSerdes() throws InvalidProtocolBufferException {
		LOGGER.trace("EnvelopeSerdesTest::testEmptyEnvelopeSerdes()");
		LOGGER.debug("Testing empty envelope serialization/deserialization");
		// setup
		Envelope expectedEnvelope = Envelope.newBuilder()
			.setId(ID)
			.build();
		// test
		byte[] bytes = serializer.serialize(TOPIC, expectedEnvelope);
		Envelope actualEnvelope = deserializer.deserialize(TOPIC, bytes);
		// verify
		assertNotNull(actualEnvelope);
		assertEquals(actualEnvelope, expectedEnvelope);
		assertEquals(actualEnvelope.getId(), ID);
	}

	@Test
	public void testEnvelopeSerdes() throws InvalidProtocolBufferException {
		LOGGER.trace("EnvelopeSerdesTest::testEnvelopeSerdes()");
		LOGGER.debug("Testing envelope serialization/deserialization");
		// setup
		TestMessage expectedMessage = TestMessage.newBuilder()
			.setPayload("TEST_PAYLOAD")
			.build();
		Envelope expectedEnvelope = Envelope.newBuilder()
			.setId(ID)
			.setContents(Any.pack(expectedMessage))
			.build();
		// test
		byte[] bytes = serializer.serialize(TOPIC, expectedEnvelope);
		Envelope actualEnvelope = deserializer.deserialize(TOPIC, bytes);
		TestMessage actualMessage = actualEnvelope.getContents().unpack(TestMessage.class);
		// verify
		assertEquals(actualEnvelope, expectedEnvelope);
		assertEquals(actualEnvelope.getId(), ID);
		assertEquals(actualMessage, expectedMessage);
		assertEquals(actualMessage.getPayload(), expectedMessage.getPayload());
	}
}
