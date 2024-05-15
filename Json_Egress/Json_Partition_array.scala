// Required inputs
val inputDataFrame = spark.read.format("csv").option("header", "true").load("/mnt/data/input_sample.csv")  // Example DataFrame
val outputFormat = "json"                             // Target file format
val outputPath = "/mnt/data/output/jsonData"          // Destination path in DBFS or mounted storage
val maxSizePerPartitionMB = "10"                      // Size per partition in MB, if specified

// Convert input format to lowercase and trim it for safety
if (outputFormat.trim.toLowerCase == "json") {

  // Check if maxSizePerPartitionMB is defined and greater than 0
  if (maxSizePerPartitionMB != null && maxSizePerPartitionMB.nonEmpty && maxSizePerPartitionMB.toInt > 0) {
    
    println("Detected non-empty maxSizePerPartitionMB input. Proceeding with partitioned write...")

    // Write initial JSON data temporarily to calculate size
    inputDataFrame.write.mode("overwrite").json(outputPath)

    // Calculate total folder size
    val writtenFiles = dbutils.fs.ls(outputPath)
    val totalSizeInBytes = writtenFiles.map(_.size).sum
    val totalSizeInMB = totalSizeInBytes / math.pow(1024, 2)
    println(s"Total data size in JSON format: $totalSizeInMB MB")

    // Calculate number of partitions based on max size
    val maxPartitionMB = maxSizePerPartitionMB.toInt
    val calculatedPartitions = math.ceil(totalSizeInMB / maxPartitionMB).toInt
    println(s"Repartitioning into $calculatedPartitions partitions (max $maxPartitionMB MB per partition)")

    // Clean old data before re-writing
    dbutils.fs.rm(outputPath, recurse = true)

    // Repartition and write again
    val resizedDF = inputDataFrame.repartition(calculatedPartitions)
    resizedDF.write.format("json").mode("overwrite").save(outputPath)

    // Function to convert JSON files to array format (list of JSON objects)
    def convertJsonToListFormat(filePath: String): String = {
      val jsonContent = spark.read.json(filePath)
      val jsonArrayString = jsonContent.toJSON.collect().mkString("[", ",", "]")
      jsonArrayString
    }

    // Modify all written JSON files to be proper JSON arrays
    val jsonFiles = dbutils.fs.ls(outputPath).filter(file => file.name.endsWith(".json"))

    jsonFiles.foreach { file =>
      val filePath = file.path
      println(s"Processing file for formatting: $filePath")
      try {
        val modifiedJsonArray = convertJsonToListFormat(filePath)
        dbutils.fs.put(filePath, modifiedJsonArray, overwrite = true)
        println(s"File successfully formatted: $filePath")
      } catch {
        case e: Exception =>
          println(s"Failed to process $filePath: ${e.getMessage}")
      }
    }

  } else {
    // If no max partition size given, write directly in append mode
    inputDataFrame.write.format("json").mode("append").save(outputPath)
  }

} else {
  throw new Exception("Invalid output format. Only parquet, csv, tsv, and json are supported.")
}
