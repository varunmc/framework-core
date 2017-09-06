package mc.varun.core;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;

@ComponentScan("mc.varun.core")
@ContextConfiguration
@PropertySource("application-test.properties")
public class TestConfiguration {
	// NOOP
}
