package io.teknek.farsandra;

import java.util.Arrays;

import org.junit.Test;

public class TestFarsandra {

  @Test
  public void simpleTest(){
    Farsandra fs = new Farsandra();
    fs.withVersion("2.0.4");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("localhost");
    fs.withSeeds(Arrays.asList("localhost"));
    fs.start();
  }
}
