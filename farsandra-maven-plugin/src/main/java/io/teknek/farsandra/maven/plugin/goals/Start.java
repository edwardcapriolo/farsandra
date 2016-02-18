package io.teknek.farsandra.maven.plugin.goals;

import io.teknek.farsandra.Farsandra;
import io.teknek.farsandra.LineHandler;
import io.teknek.farsandra.ProcessHandler;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.teknek.farsandra.maven.plugin.Constants.FARSANDRA;

/**
 * Start goal for Farsandra Maven Plugin.
 *
 * @author Andrea Gazzarini
 * @since 1.0
 */
@Mojo(name = "start")
public class Start extends AbstractMojo {

	@Parameter(required = true)
	private String version;

	@Parameter(defaultValue = "true")
	private boolean cleanInstanceOnStart;

	@Parameter(defaultValue = "${basedir}/target/cassandra")
	private String instanceName;

	@Parameter(defaultValue = "true")
	private boolean createConfigurationFiles;

	@Parameter(defaultValue = "localhost")
	private String host;

	@Parameter
	private int port;

	@Parameter
	private int jmxPort;

	@Parameter
	private String javaHome;

	@Parameter
	private List<String> seeds;

	@Parameter
	private List<String> additionalEnvLines = Collections.emptyList();

	@Parameter
	private List<String> additionalYamlLines = Collections.emptyList();

	@Parameter
	private Map<String, String> yamlReplacements = Collections.emptyMap();

	@Parameter(defaultValue = "false")
	private boolean stdOutEnabled;

	@Parameter(defaultValue = "false")
	private boolean stdErrEnabled;

	@Parameter(defaultValue = "Listening for thrift clients...")
	private String upAndRunningMessage;

	Farsandra farsandra;

	/**
	 * Builds a new instance of Start Mojo.
	 */
	public Start() {
		farsandra = new Farsandra();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute() throws MojoExecutionException, MojoFailureException {
		farsandra
			.withVersion(version)
			.withInstanceName(instanceName)
			.withCleanInstanceOnStart(cleanInstanceOnStart)
			.withCreateConfigurationFiles(createConfigurationFiles);

//		for (final Map.Entry<String, String> entry : yamlReplacements.entrySet()) {
//			farsandra.withYamlReplacement(entry.getKey(), entry.getValue());
//		}

		for (final String line : additionalYamlLines) {
			farsandra.appendLineToYaml(line);
		}

		for (final String line : additionalEnvLines) {
			farsandra.appendLinesToEnv(line);
		}

		final CountDownLatch started = new CountDownLatch(1);

		if (stdErrEnabled) {
			farsandra.getManager().addErrLineHandler(
					new LineHandler() {
						@Override
						public void handleLine(final String line) {
							System.err.println(line);
						}
					});
		}

		farsandra.getManager().addOutLineHandler(
				new LineHandler() {
					@Override
					public void handleLine(final String line) {
						if (stdOutEnabled) {
							System.out.println(line);
						}

						if (line.contains(upAndRunningMessage)) {
							started.countDown();
						}
					}
				});

		farsandra.getManager().addProcessHandler(
                new ProcessHandler() {
                    @Override
                    public void handleTermination(final int exitValue) {
                        started.countDown();
                    }
                }
        );

		getPluginContext().put(FARSANDRA, farsandra);

		try {
			farsandra.start();
			started.await();
		} catch (final Exception exception) {
			getLog().error(exception);
			throw new MojoFailureException("Unable to start Farsandra. See the stacktrace above for further details...");
		}
	}

	/**
	 * Set the version of Cassandra.
	 * Should be a string like "2.0.4"
	 *
	 * @param version the version of Cassandra.
	 */
	public void setVersion(final String version) {
		this.version = version;
	}

	/**
	 * Enables / disables the standard out.
	 *
	 * @param enable true if the standard out has to be enabled, false otherwise.
	 */
	public void setStdOutEnabled(final boolean enable) {
		this.stdOutEnabled = enable;
	}

	/**
	 * Enables / disables the standard err.
	 *
	 * @param enable true if the standard err has to be enabled, false otherwise.
	 */
	public void setStdErrEnabled(final boolean enable) {
		this.stdErrEnabled = enable;

	}

	/**
	 * Sets the list of seed nodes.
	 * It defaults to a list with just one "localhost" seed node.
	 *
	 * @param seeds the list of seed nodes.
	 */
	public void setSeeds(final List<String> seeds) {
		farsandra.withSeeds(seeds == null || seeds.isEmpty() ? Arrays.asList("localhost") : seeds);
	}

	/**
	 * Sets the value of the JAVA_HOME environment variable.
	 *
	 * @param javaHome the value of the JAVA_HOME environment variable.
	 */
	public void setJavaHome(final String javaHome) {
		this.javaHome = javaHome; // Not actually needed, just to avoid confusion.
		farsandra.withJavaHome(javaHome);
	}

	/**
	 * Sets the JMX listening port.
	 *
	 * @param jmxPort the JMX listening port.
	 */
	public void setJmxPort(final int jmxPort) {
		this.jmxPort = jmxPort; // Not actually needed, just to avoid confusion.
		farsandra.withJmxPort(jmxPort);
	}

	/**
	 * Sets the RPC listening port.
	 *
	 * @param port the RPC listening port
	 */
	public void setPort(final int port) {
		this.port = port; // Not actually needed, just to avoid confusion.
		farsandra.withPort(port);
	}

	/**
	 * Sets the listen address (hostname or IP).
	 * It defaults to "localhost".
	 *
	 * @param host the listen address (hostname or IP).
	 */
	public void setHost(final String host) {
		this.host = host; // Not actually needed, just to avoid confusion.
		farsandra.withHost(host);
	}

	/**
	 * If true, any pre-existing instance directory will be deleted.
	 *
	 * @param cleanInstanceOnStart true if any pre-existing instance directory has to be deleted, false otherwise.
	 */
	public void setCleanInstanceOnStart(final boolean cleanInstanceOnStart) {
		this.cleanInstanceOnStart = cleanInstanceOnStart;
	}

	/**
	 * If true, Farsandra will generate the conf directory (with all configuration files).
	 *
	 * @param createConfigurationFiles true if Farsandra has to generate the conf directory and the configuration files, false otherwise.
	 */
	public void setCreateConfigurationFiles(final boolean createConfigurationFiles) {
		this.createConfigurationFiles = createConfigurationFiles;
	}

	/**
	 * Sets the instance name.
	 * This will also be the data directory where the instance is found.
	 * It defaults to ${basedir}/target/cassandra
	 *
	 * @param instanceName the instance name.
	 */
	public void setInstanceName(final String instanceName) {
		this.instanceName = instanceName != null && instanceName.trim().length() > 0
				? instanceName
				: this.instanceName;
	}
}