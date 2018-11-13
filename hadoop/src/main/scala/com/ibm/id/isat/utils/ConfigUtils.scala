package com.ibm.id.isat.utils

import com.typesafe.config.ConfigFactory

/**
  * This singleton class is provide an abstraction to access properties defined in the classpath
  * The <code>properties</code> are loaded in the form of (firs-listed are higher priority)
  * <li>system properties (java property)</li>
  * <li>application.conf</li>
  * <li>application.json</li>
  * <li>application.properties></li>
  */
object ConfigUtils {

  private val config = ConfigFactory.load()


  /**
    * return the value to which specific key is map and return <param>default</param> otherwise (no mapping found)
    *
    */
  def getValueAsString(key: String, default: String = "") = {
    try {
      config.getString(key)
    } catch {
      case e: Exception => default
    }
  }

  /**
    * return the element of specific location of the key
    * (i.e given a key in the form of <code>key=["val_1", "val_2"]<code>, calling this method with
    * <param>key</param> and <param>0</param> will return val_1
    *
    */
  def getValueFromList(key: String, index: Int) = {
    try {
      config.getStringList(key).get(index)
    } catch {
      case e: Exception => ""
    }
  }

  /**
    * @see ConfigUtils#getValueAsString
    *
    */
  def getValueAsInt(key: String, default: Int = 0) = {
    try {
      config.getInt(key)
    } catch {
      case e: Exception => 0
    }

  }
  /**
    * @see ConfigUtils#getValueFromList
    *
    */
  def getValueAsIntFromList(key:String, index : Int, default :Int = 0) = {
    try{
      config.getStringList(key).get(index)
    }catch {
      case e: Exception => default

    }
  }
}
