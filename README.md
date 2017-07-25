# camel-gearman
a camel gearman component
## simple usage
<pre><code>
String from = "gearman:gearman.host:4730/FUNCTION_NAME?charset=utf-8&workerThread=15&threadTimeout=30000";
</code></pre>
- `gearman.host`  : gearman job server host
- `FUNCTION_NAME` : gearman function name , like a queue name
- `workerThread`  : worker thread number in worker thread pool
- `threadTimeout` : max idle time of the worker thread in worker thread pool 

## maven 
```xml
<repository>
  <id>camel-gearman-mvn-repo</id>
  <url>https://raw.github.com/eXcellme/camel-gearman/mvn-repo/</url>
  <snapshots>
    <enabled>true</enabled>
    <updatePolicy>always</updatePolicy>
  </snapshots>
</repository>

<dependency>
  <groupId>org.apache.camel</groupId>
  <artifactId>camel-gearman</artifactId>
  <version>2.19.1</version>
</dependency>
```

thanks to [gearman/java-service](https://github.com/gearman/java-service)!