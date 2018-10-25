## PrestoRunner can execute Presto script from S3 by custom jar step of EMR. 

* Use presto-cli or Presto JDBC driver

### Usage
* Add custom jar step on EMR
* Execute jar manually ↓
```
hadoop jar PrestoRunner.jar #{s3 path} #{ cli | jdbc(default) }
```
