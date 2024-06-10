// Databricks notebook source
import scalaj.http.{Http, HttpOptions, HttpResponse}
import org.apache.spark.sql.{Dataset, SparkSession}
import spark.implicits._


// COMMAND ----------

val private_key = "ghhfdhj gadsjhc gasdjhc AJHDFVc gaHFDc=="

val domain = "https://login.salesforce.com/"
val tokenURL = "https://login.salesforce.com/services/oauth2/token"

val consumer_key = "abcdefghijklmnoplmnopqrstuvwxyzgdsagvjhfgajhgfjhkgdfhasbjhfbjhVFDSGJHbdfcjshbhvbajhfv"
                   

val userId = "me@mycompany.com"
val sourceURL = "https://mycompany.my.salesforce.com/services/data/v47.0/jobs/query"

var selectQuery = "select a,b,c from my_table"

// COMMAND ----------

import org.apache.commons.codec.binary.Base64
import java.security.Signature
import scala.util.{Try, Success, Failure}
import java.security.{PrivateKey}
""
var header = """{"alg":"RS256"}"""
var encodedHeader = Base64.encodeBase64URLSafeString(header.getBytes("utf-8"))

val expiretime = ((System.currentTimeMillis()/1000) + 300).toString

println(expiretime)
var payload = """{"iss":"""" + consumer_key + """","aud":"""" + domain + """","sub":"""" + userId + """","exp":"""" + expiretime + """"}"""
var encodedpayload = Base64.encodeBase64URLSafeString(payload.getBytes("utf-8"))

var message = encodedHeader + "." + encodedpayload


// ***************************************************

def removeBeginEnd(pem: String) = {
    pem.replaceAll("-----BEGIN (.*)-----", "")
      .replaceAll("-----END (.*)----", "")
      .replaceAll("\r\n", "")
      .replaceAll("\n", "")
      .trim()
  }

def pemToDer(pem: String) = {
    val removedpem = removeBeginEnd(pem)
    Base64.decodeBase64(removedpem)
  } 

def bytesToPrivateKeyObject(der: Array[Byte]): PrivateKey = {
   
    import java.security.spec.PKCS8EncodedKeySpec
    import java.security.KeyFactory
    import java.security.Security
//     import org.bouncycastle.jce.provider.BouncyCastleProvider
    
//     if (Security.getProvider("BC") == null) Security.addProvider(new BouncyCastleProvider())

    val spec = new PKCS8EncodedKeySpec(der)
    val kf = KeyFactory.getInstance("RSA")
    kf.generatePrivate(spec);
  }

def decodePrivateKey(pem: String): PrivateKey = {
      val bytes = pemToDer(pem)
      bytesToPrivateKeyObject(bytes)
  }

// *****************************************

val rsa = Signature.getInstance("SHA256withRSA")
rsa.initSign(decodePrivateKey(private_key))
rsa.update(message.getBytes("utf-8"))
var result = Base64.encodeBase64URLSafeString(rsa.sign())

var data = message + "." + result

// COMMAND ----------


// **********  GET Access Token *************

val tokenresponse = Http(tokenURL).timeout(connTimeoutMs = 20000, readTimeoutMs = 600000).method("post")
  .param("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
  .param("assertion", data)
  .header("Content-Type", "application/x-www-form-urlencoded")
  .header("Charset", "UTF-8").asString

val tokends = spark.read.json(Seq(tokenresponse.body).toDS())

//display(tokends)
val token = tokends.select("access_token").take(1)(0)(0).toString()
println(token)

// COMMAND ----------


// ***************** Create Job **************** 
val createjobResp = Http(sourceURL).timeout(connTimeoutMs = 20000, readTimeoutMs = 600000)
  .postData("""{
  "operation": "query",
  "query": """" + selectQuery + """"
  }""")
  .header("Authorization", "Bearer " + token)
  .header("Content-Type", "application/json")
  .header("Charset", "UTF-8").asString

val job = spark.read.json(Seq(createjobResp.body).toDS())
val jobId = job.select("id").take(1)(0)(0).toString()


println(jobId)

//display(job)

// COMMAND ----------

val jobStatus = Http(sourceURL + "/" + jobId).timeout(connTimeoutMs = 20000, readTimeoutMs = 600000)
  .header("Authorization", "Bearer " + token)
  .header("Content-Type", "application/json")
  .header("Charset", "UTF-8").asString

val statusresponseBody = spark.read.json(Seq(jobStatus.body).toDS())
val status = statusresponseBody.select("state").take(1)(0)(0).toString()
val totalProcessedRecord = statusresponseBody.select("numberRecordsProcessed").take(1)(0)(0).toString()

println(status)

// COMMAND ----------

val result = Http(sourceURL + "/"  + jobId + "/results?maxRecords=100000").timeout(connTimeoutMs = 20000, readTimeoutMs = 600000)
  .header("Authorization", "Bearer " + token)
  .asString

val resultBody = result.body
//println(resultBody)

val csvData: Dataset[String] = spark.sparkContext.parallelize(resultBody.stripMargin.lines.toList).toDS()
val Dframe = spark.read.option("header", true).csv(csvData)

display(Dframe)

//   val stringList = resultBody.replaceAll( ""[\n.]+"", "").split("\n").toList
//   val csvData: Dataset[String] = spark.sparkContext.parallelize(stringList).toDS()
//   val resultDF = spark.read.option("header", true).csv(csvData)

// COMMAND ----------

Dframe.count

// COMMAND ----------

display(Dframe)


// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct load_dt from my_salesforce_company.unit

// COMMAND ----------


