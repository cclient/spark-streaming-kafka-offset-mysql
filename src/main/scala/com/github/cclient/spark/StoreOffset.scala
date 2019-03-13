package com.github.cclient.spark

case class StoreOffset(topic: String, group: String, step: Int, partition: Int, from: Long, until: Long, count: Long, datetime: String)