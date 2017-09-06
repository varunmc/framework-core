package mc.varun.core;

import static org.testng.Assert.assertEquals;

import com.google.protobuf.Message;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import mc.varun.core.api.MessageHandler;
import mc.varun.core.api.Messenger;
import mc.varun.proto.EnvelopeOuterClass.Envelope;
import mc.varun.proto.TestMessageOuterClass.TestMessage;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Tests the streaming of messages.
 *
 * TODO: The first test run always fails while subsequent runs succeed. Needs investigation.
 */
@ContextConfiguration(classes = TestConfiguration.class)
public final class MessageStreamTest extends AbstractTestNGSpringContextTests implements MessageHandler<TestMessage> {

	private static final Logger LOGGER = LoggerFactory.getLogger(MessageStreamTest.class);

	private static final long TIMEOUT_MS = 10000;

	private static final int TOTAL_MESSAGES = 10;

	private Queue<TestMessage> expectedMessages = new ArrayBlockingQueue<>(TOTAL_MESSAGES);

	@Autowired
	private Messenger messageBus;

	@BeforeSuite
	public void beforeSuite() throws Exception {
		LOGGER.trace("MessageStreamTest::beforeSuite()");
		super.springTestContextPrepareTestInstance();
		messageBus.register(this, TestMessage.class);
	}

	@BeforeTest
	public void beforeTest() {
		LOGGER.trace("MessageStreamTest::beforeTest()");
		messageBus.start();
	}

	@Override
	public KStream<Envelope, ? extends Message> handle(KStream<Envelope, TestMessage> stream) {
		LOGGER.trace("MessageStreamTest::handle({})", stream);
		return stream.mapValues(actualMessage -> {
			TestMessage expectedMessage = expectedMessages.peek();
			if (!actualMessage.equals(expectedMessage)) {
				return null;
			}
			expectedMessages.remove();
			assertEquals(actualMessage.getPayload(), expectedMessage.getPayload());
			if (expectedMessages.isEmpty()) {
				synchronized (this) {
					notifyAll();
				}
			}
			return null;
		});
	}

	@Test
	public void testMessageStream() throws InterruptedException {
		LOGGER.trace("MessageStreamTest::testMessageStream()");
		LOGGER.debug("Testing message streaming");
		// setup
		for (int count = 0; count < TOTAL_MESSAGES; count++) {
			TestMessage message = TestMessage.newBuilder()
				.setPayload(UUID.randomUUID().toString())
				.build();
			expectedMessages.add(message);
			messageBus.send(message);
		}
		synchronized (this) {
			wait(TIMEOUT_MS);
		}
		// verify
		assertEquals(expectedMessages.size(), 0, expectedMessages.size() + "/" + TOTAL_MESSAGES + " messages left to process -");
	}
}
