package com.ibm.id.isat.usage.cs3

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.io._

/**
  * This singleton class is provide an abstraction to access properties defined in the classpath
  * The <code>properties</code> are loaded in the form of (firs-listed are higher priority)
  * <li>system properties (java property)</li>
  * <li>application.conf</li>
  * <li>application.json</li>
  * <li>application.properties></li>
  */
object TestConfig {

  private val config = ConfigFactory.load("Test")
  
    
  def main(args: Array[String]): Unit = {
    
    //val sc = new SparkContext("local", "Config", new SparkConf());
    
    //println(config.getObject("path").toString())
    //println(config.getString("location"))
    //ConfigFactory.parseReader(reader)
    val value = ConfigFactory.parseReader(new InputStreamReader(new FileInputStream(new File("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Workspace/hadoop/target/classes/Test.conf")))).getString("my.secret.value");
    //val value = ConfigFactory.load("com/ibm/id/isat/usage/cs3/Test").getString("my.secret.value")
    println(s"My secret value is $value")
    
    val sc = new SparkContext("local", "Config", new SparkConf());
    val hadoopConfig: Configuration = sc.hadoopConfiguration
    val fs: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConfig)
    val file: FSDataInputStream = fs.open(new Path("C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Workspace/hadoop/target/classes/Test.conf"))
    val reader = new InputStreamReader(file)
    val config = ConfigFactory.parseReader(reader)
    println(config.getString("test"))
    /*
    val sc = new SparkContext("local", "Config", new SparkConf());
    //val sc = new SparkContext(new SparkConf())
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val configDir = "C:/Users/IBM_ADMIN/Documents/BENA/GBS/Indosat/Hadoop/Hadoop New Development/Sample CS3/CS3_TRANSFORM.conf"
       
    println(getPathDirecory(sc, configDir, "INPUT_DIR"))
    println(getPathDirecory(sc, configDir, "OUTPUT_DIR"))
    println("TEST : " + getPathDirecory(sc, configDir, "OUTPUT_DIR"))
    
    */
  }
  
  def getPathDirecory2(sc: SparkContext, path: String, key: String) : String = {
    
    try {
        
        val hadoopConfig: Configuration = sc.hadoopConfiguration
        val fs: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConfig)
        val file: FSDataInputStream = fs.open(new Path(path))
        val reader = new InputStreamReader(file)
        val config = ConfigFactory.parseReader(reader)
        
        return config.getString(key)
    }
    
    catch {
      case e: Exception => throw new Exception("Key : " + key + " => " + e.getMessage)         
    }
  }
  
  def getPathDirecory(sc: SparkContext, path: String, key: String) : String = {
    
    try {
        val fileConfigRDD = sc.textFile(path)
        val fileConfigPairRDD = fileConfigRDD.map(line => (line.split("\\=")(0),line.split("\\=")(1)))
        fileConfigPairRDD.persist()
        val filterConfigRDD = fileConfigPairRDD.filter{case (k,v) => k == key}
        
        return filterConfigRDD.values.first()
    }
    
    catch {
      case e: Exception => throw new Exception("Key : " + key + " => " + e.getMessage)         
    }
  }
  
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
