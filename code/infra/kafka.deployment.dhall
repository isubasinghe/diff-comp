
let kubernetes = https://raw.githubusercontent.com/dhall-lang/dhall-kubernetes/master/1.17/package.dhall sha256:532e110f424ea8a9f960a13b2ca54779ddcac5d5aa531f86d82f41f8f18d7ef1
let env = ./kafka.env.dhall
let zooEnv = ./zookeeper.env.dhall

let zookeeperURI = "${zooEnv.zookeeperName}:${Integer/show zooEnv.zookeeperPort}"

let deployment = 
    kubernetes.Deployment::{
      , metadata = kubernetes.ObjectMeta::{ name = Some env.selector}
      , spec = Some kubernetes.DeploymentSpec::{
        , selector = kubernetes.LabelSelector::{
          , matchLabels = Some (toMap { app = env.selector })
          }
        , replicas = Some +2
        , template = kubernetes.PodTemplateSpec::{
          , metadata = Some kubernetes.ObjectMeta::{ labels = Some (toMap { app = env.selector }) }
          , spec = Some kubernetes.PodSpec::{
            , containers =
              [ kubernetes.Container::{
                , name = env.kafkaName
                , image = Some "wurstmeister/kafka"
                , ports = Some
                  [ kubernetes.ContainerPort::{ containerPort = env.kafkaPort } ]
                , env = Some
                  [ 
                    , kubernetes.EnvVar::{ 
                      , name = "KAFKA_ADVERTISED_PORT"
                      , value = Some "30718"
                    }
                    , kubernetes.EnvVar::{
                      , name = "KAFKA_ADVERTISED_HOST_NAME"
                      , value = Some "192.168.1.240"
                    }
                    , kubernetes.EnvVar::{
                      , name = "KAFKA_ZOOKEEPER_CONNECT"
                      , value = Some zookeeperURI
                    }
                    , kubernetes.EnvVar::{
                      , name = "KAFKA_BROKER_ID"
                      , value = Some env.id
                    }
                    , kubernetes.EnvVar::{
                      , name = "KAFKA_CREATE_TOPICS"
                      , value = Some ""
                    }
                  ]
                }
              ]
            }
          }
        }
      }
in  deployment

