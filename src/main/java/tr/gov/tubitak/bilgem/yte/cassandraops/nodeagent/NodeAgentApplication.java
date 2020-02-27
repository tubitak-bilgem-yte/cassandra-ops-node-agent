package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class NodeAgentApplication {
	public static void main(final String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(NodeAgentApplication.class, args);
	}
}
