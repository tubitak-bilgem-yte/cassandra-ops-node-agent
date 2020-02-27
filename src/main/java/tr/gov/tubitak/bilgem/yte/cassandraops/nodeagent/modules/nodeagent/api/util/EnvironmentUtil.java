package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class EnvironmentUtil {
    private Environment environment;

    public int getAgentPort() {
        return Integer.parseInt(environment.getProperty("server.port"));
    }

    public String getClusterName() {
        return environment.getProperty("nodeagent.clustername").trim();
    }

    public String getNodeName() {
        return environment.getProperty("nodeagent.nodename").trim();
    }

    public String getNodePublicIpAddress() {
        return environment.getProperty("nodeagent.publicipaddress").trim();
    }

    public String getCassandraUserName() {
        return environment.getProperty("cassandra.username").trim();
    }

    public String getCassandraPassword() {
        return environment.getProperty("cassandra.password").trim();
    }

    public String getCassandraBinDir() {
        return environment.getProperty("cassandra.cassandrabindir").trim();
    }

    public String getMasterPublicIpAddress() {
        return environment.getProperty("cassandraopsmaster.publicipaddress").trim();
    }

    public int getMasterPort() {
        return Integer.parseInt(environment.getProperty("cassandraopsmaster.port"));
    }

    @Autowired
    protected void setEnvironment(final Environment environment) {
        this.environment = environment;
    }
}
