farsandra
=========

Slogan "Keep Cassandra close, but your classpath closer"

Run cassandra inside a java project without bring server deps into client classpath.

Farsandra works by downloading and automatically unpacking a cassandra inside $HOME/.farsandra. It then uses some clever string replacement to edit the yaml file, and start the service in the foreground.

Usage
========

Generally you will be using farsandra inside a unit test.

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
    fs.start();
    started.await();
    System.out.println("Thrift open. Party time!");
    Thread.sleep(10000);
    fs.getManager().destroy();
