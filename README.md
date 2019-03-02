### use Spark SQL to load/store offset with mysql

less complicated than custom implement sql operation

#### offset store

```sql
mysql> select * from kfk_offset where datetime>'2019-01-09' and topic='task-response' and `group`='extract';
+--------+------------+-------+------+-----------+-----------+-----------+-------+---------------------+
| id     | topic      | group | step | partition | from      | until     | count | datetime            |
+--------+------------+-------+------+-----------+-----------+-----------+-------+---------------------+
| 1 | task-response | extract   |  1 |         0 | 1959008 | 1995008 | 36000 | 2019-01-09 00:01:19 |
| 2 | task-response | extract   |  1 |         1 | 1897546 | 1933546 | 36000 | 2019-01-09 00:01:19 |
| 0 | task-response | extract   |  1 |         2 | 1876072 | 1912072 | 36000 | 2019-01-09 00:01:19 |
| 5 | task-response | extract   |  2 |         0 | 1995008 | 2031008 | 36000 | 2019-01-09 00:05:05 |
| 7 | task-response | extract   |  2 |         1 | 1933546 | 1969546 | 36000 | 2019-01-09 00:05:05 |
| 6 | task-response | extract   |  2 |         2 | 1912072 | 1948072 | 36000 | 2019-01-09 00:05:05 |

```

For my scene(extract crawler's response dom/json),I need rollback to the 'problem datetime' and re-consumer records after it;

### rollback
 
1 kill the spark consume process

2 point the problem datetime,then delete sql record by datetime or step 

`delete from kfk_offset where `step`>1 and `topic`='task-response' and `group`='extract'`

3 start the spark consume process

### Use 

#### develop 

copy source code

or

copy spark-streaming-kafka-offset-mysql_2.11-0.1.jar -> {project}/lib/

#### deploy

sbt package

copy spark-streaming-kafka-offset-mysql_2.11-0.1.jar -> $SPARK_HOME/jars/

### other

upload to maven repositories to use jar 