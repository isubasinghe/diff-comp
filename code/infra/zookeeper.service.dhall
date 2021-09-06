
let kubernetes = https://raw.githubusercontent.com/dhall-lang/dhall-kubernetes/master/1.17/package.dhall sha256:532e110f424ea8a9f960a13b2ca54779ddcac5d5aa531f86d82f41f8f18d7ef1
let env = ./zookeeper.env.dhall

let spec =
      { selector = Some (toMap { app = env.selector })
      , type = Some "NodePort"
      , ports = Some
        [ kubernetes.ServicePort::{
          , name = Some "client"
          , port = env.zookeeperClientPort
          , protocol = Some "TCP"
          }
          , kubernetes.ServicePort::{
            , name = Some "follower"
            , port = env.zookeeperFollowerPort
            , protocol = Some "TCP" 
          }
          , kubernetes.ServicePort::{
            , name = Some "leader"
            , port = env.zookeeperLeader
            , protocol = Some "TCP"
          }
        ]
      }

let service
    : kubernetes.Service.Type
    = kubernetes.Service::{
      , metadata = kubernetes.ObjectMeta::{
        , name = Some env.zookeeperName
        , labels = Some (toMap { app = env.selector })
        }
      , spec = Some kubernetes.ServiceSpec::spec
      }

in  service