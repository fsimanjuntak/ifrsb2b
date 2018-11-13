package com.ibm.id.isat.utils


class ConfigUtilsTest {

  import org.hamcrest.{CoreMatchers, MatcherAssert}
  import org.junit.Test

  @Test
  def config(): Unit = {


    MatcherAssert.assertThat("the value of traffic case roaming key 0 with index 0 is non roaming",
      "non roaming", CoreMatchers.is(ConfigUtils.getValueFromList("trafficaseroam.0", 0)))

    MatcherAssert.assertThat("the value of traffic case roaming key 12 with index 1 is incoming",
      "incoming", CoreMatchers.is(ConfigUtils.getValueFromList("trafficaseroam.12", 1)))

    MatcherAssert.assertThat("the value of simpleKey is simpleValue",
      "simpleValue", CoreMatchers.is(ConfigUtils.getValueAsString("simpleKey")))


  }
}
