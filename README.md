# Social network feed analyzing app
This is sample app showing usage of spark sql and how to analyze social network feeds information.
## Data format
This project contains feeds_show.json file with following schema:
```
root
 |-- durationMs: long (nullable = true)
 |-- owners: struct (nullable = true)
 |    |-- group: array (nullable = true)
 |    |    |-- element: long (containsNull = true)
 |    |-- user: array (nullable = true)
 |    |    |-- element: long (containsNull = true)
 |-- platform: string (nullable = true)
 |-- position: long (nullable = true)
 |-- resources: struct (nullable = true)
 |    |-- GROUP_PHOTO: array (nullable = true)
 |    |    |-- element: long (containsNull = true)
 |    |-- MOVIE: array (nullable = true)
 |    |    |-- element: long (containsNull = true)
 |    |-- POST: array (nullable = true)
 |    |    |-- element: long (containsNull = true)
 |    |-- USER_PHOTO: array (nullable = true)
 |    |    |-- element: long (containsNull = true)
 |-- timestamp: long (nullable = true)
 |-- userId: long (nullable = true)
 ```
The input schema described in `Main.scala` object

## Metrics
In this app following metrics are collected and shown:
* Count of views and users grouped by platforms
* Summary count of views and users
* Daily unique authors and content
* Views grouped by sessions and some information about these sessions (count, average duration, viewing depth)
* Views grouped by users

## How to run
You can use one of the following variants:
* run in Intellij IDEA or Eclipse IDE using `Main.scala` class
* run `sbt run`
