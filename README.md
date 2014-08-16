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

