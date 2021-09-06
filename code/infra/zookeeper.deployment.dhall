
let kubernetes = https://raw.githubusercontent.com/dhall-lang/dhall-kubernetes/master/1.17/package.dhall sha256:532e110f424ea8a9f960a13b2ca54779ddcac5d5aa531f86d82f41f8f18d7ef1
let env = ./zookeeper.env.dhall
let deployment = 
    kubernetes.Deployment::{
      , metadata = kubernetes.ObjectMeta::{ name = Some env.selector }
      , spec = Some kubernetes.DeploymentSpec::{
        , selector = kubernetes.LabelSelector::{
          , matchLabels = Some (toMap { name = env.selector })
          }
        , replicas = Some +2
        , template = kubernetes.PodTemplateSpec::{
          , metadata = Some kubernetes.ObjectMeta::{ name = Some env.selector }
          , spec = Some kubernetes.PodSpec::{
            , containers =
              [ kubernetes.Container::{
                , name = "nginx"
                , image = Some "nginx:1.15.3"
                , ports = Some
                  [ kubernetes.ContainerPort::{ containerPort = env.zookeeperPort } ]
                , env = Some
                  [ 
                    , kubernetes.EnvVar::{ 
                      , name = "ZOOKEEPER_ID"
                      , value = Some env.zookeeperID
                    }
                    , kubernetes.EnvVar::{
                      , name = "ZOOKEEPER_SERVER_1"
                      , value = Some "zoo1"
                    } 
                  ]
                }
              ]
            }
          }
        }
      }
in  deployment
