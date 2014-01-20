package io.teknek.farsandra;

import org.junit.Test;

public class TestFarsandra {

  @Test
  public void simpleTest(){
    Farsandra fs = new Farsandra();
    fs.withVersion("2.0.4");

    fs.withInstanceName("1");
    fs.start();
  }
}
