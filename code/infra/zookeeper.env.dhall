
let selector = "zookeeper-kafka"
let zookeeperPort = +2081
let zookeeperID = "1"
let zookeeperName = "zookeeper-kafka-1"
let zookeeperClientPort = +2181
let zookeeperFollowerPort = +2888
let zookeeperLeader = +3888
in { selector = selector
   , zookeeperPort = zookeeperPort
   , zookeeperID = zookeeperID
   , zookeeperName = zookeeperName
   , zookeeperClientPort = zookeeperClientPort
   , zookeeperFollowerPort = zookeeperFollowerPort
   , zookeeperLeader = zookeeperLeader
   }