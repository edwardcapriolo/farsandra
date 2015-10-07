farsandra
=========

Slogan "Keep Cassandra close, but your classpath closer"

Run cassandra inside a java project without bring server deps into client classpath.

Farsandra works by downloading and automatically unpacking a cassandra inside $HOME/.farsandra. It then uses some clever string replacement to edit the yaml file, and start the service in the foreground.

You may be saying to yourself: Why would I want to do that? Can't I run cassandra embedded? Well you could, but C* is not re-entrant. You really can't start and stop it, and it is awkward to clean it out each test. Besides that it brings several libraries onto your classpath which you do not need from a client prospective and can cause classpath issues with your code.

Usage
========

Generally you will be using farsandra inside a unit test.

    Farsandra fs = new Farsandra();
    fs.withVersion("2.0.3");
    ...
    fs.start();
    started.await();
    System.out.println("Thrift open. Party time!");
    Thread.sleep(10000);
    fs.getManager().destroy();

Customizing Cassandra.yaml
===========

Farsandra has methods to allow setting the common parameters inside the yaml file. A simple approach is to append lines ot the bottom. 

    fs.appendLineToYaml("#this means nothing");

Another option is to search replace

    fs.withYamlReplacement("# NOTE:", "# deNOTE:");

Customizing Cassandra-env.sh
============

We can also append lines to the env file.

    fs.appendLinesToEnv("#this also does nothing");

Or do replacements

    fs.withEnvReplacement("#MALLOC_ARENA_MAX=4", "#MALLOC_ARENA_MAX=wombat");

Putting listeners on the output and error streams
========

    fs.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started.countDown();
          }
        }
      }
    );
    fs.getManager().addErrLineHandler( new LineHandler(){
      @Override
      public void handleLine(String line) {
        System.out.println("err "+line);
      }
    });

Adding an handler to manage process termination
==========

    fs.getManager().addProcessHandler(new ProcessHandler() {
        @Override
        public void handleTermination(int exitValue) {
            System.out.println("Cassandra terminated with exit value: " + exitValue);
            started.countDown();
        }
    });

Puttingit all together
==========

    Farsandra fs = new Farsandra();
    fs.withVersion("2.0.3");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("localhost");
    fs.withSeeds(Arrays.asList("localhost"));
    final CountDownLatch started = new CountDownLatch(1);
    fs.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started.countDown();
          }
        }
      }
    );
    fs.getManager().addErrLineHandler( new LineHandler(){
      @Override
      public void handleLine(String line) {
        System.out.println("err "+line);
      }
    });
    fs.getManager().addProcessHandler(new ProcessHandler() {
        @Override
        public void handleTermination(int exitValue) {
            System.out.println("Cassandra terminated with exit value: " + exitValue);
            started.countDown();
        }
    });
    fs.start();
    started.await();
    System.out.println("Thrift open. Party time!");
    Thread.sleep(10000);
    fs.getManager().destroy();

# Farsandra Maven Plugin
The project also includes a Maven plugin that takes care of starting / stopping Farsandra (i.e. Cassandra) before and after the integration-test phase.  

## How to configure
In your pom.xml, you have to declare the plugin like this:        
```xml
    <build>
        <plugins>
            <plugin>
                <groupId>org.gazzax.labs</groupId>
                <artifactId>farsandra-maven-plugin</artifactId>
                <version>0.1</version>
                <configuration>
                    <version>2.0.3</version>
                    <instanceName>target/cassandra</instanceName>
                    <stdOutEnabled>false</stdOutEnabled>
                    <stdErrEnabled>false</stdErrEnabled>
                </configuration>
                <executions>
                    <execution>
                        <id>start-cassandra</id>
                        <phase>pre-integration-test</phase>
                        <goals>
                            <goal>start</goal>
                        </goals>
                    </execution>
                    <execution>
                        <id>stop-cassandra</id>
                        <phase>post-integration-test</phase>
                        <goals>
                            <goal>stop</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```
    
That's all: running a build in your project will trigger Farsandra respectively in the pre and post integration-test phases, starting and stopping the requested Cassandra version (see the version parameter) around your integration test suite.        
The following table summarizes the configuration parameters available with the current version. 

| Name | Description | Default value |
----|------|----|
|version| The Cassandra version| N.A. (mandatory)| 
|cleanInstanceOnStart | If true, cleans the instance directory| true|
|instanceName| The instance name, | "target/cassandra"|
|createConfigurationFiles| Creates the Cassandra configuration files| true|
|host|the instance listen address (hostname or IP)| "localhost"|
|seeds |A list of seed nodes| ["localhost"]|  
|port|The RPC listening port|N.A.|
|jmxPort|The JMX listening port|N.A|
|additionalEnvLines|A list of Additional lines to be appended in Cassandra environment file|N.A|
|additionalYamlLines|A list of additional lines to be appended in Cassandra configuration (yaml) file|N.A|
|yamlReplacements|A map of key/value replacements pairs. The key is a line that will be replaced with the corresponding value in the cassandra.yaml configuration file| N.A|
|javaHome|The value of the JAVA_HOME environment variable| N.A.|
|stdErrEnabled|Enables / disables the standard err| false|
|stdOutEnabled|Enables / disables the standard out| false|

