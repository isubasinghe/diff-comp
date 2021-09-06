
let kubernetes = https://raw.githubusercontent.com/dhall-lang/dhall-kubernetes/master/1.17/package.dhall sha256:532e110f424ea8a9f960a13b2ca54779ddcac5d5aa531f86d82f41f8f18d7ef1
let env = ./zookeeper.env.dhall

let spec =
      { selector = Some (toMap { app = env.selector })
      , type = Some "NodePort"
      , ports = Some
        [ kubernetes.ServicePort::{
          , name = Some "client"
          , port = +2181
          , protocol = Some "TCP"
          }
          , kubernetes.ServicePort::{
            , name = Some "follower"
            , port = +2888
            , protocol = Some "TCP" 
          }
          , kubernetes.ServicePort::{
            , name = Some "leader"
            , port = +3888
            , protocol = Some "TCP"
          }
        ]
      }

let service
    : kubernetes.Service.Type
    = kubernetes.Service::{
      , metadata = kubernetes.ObjectMeta::{
        , name = Some "zoo1"
        , labels = Some (toMap { app = env.selector })
        }
      , spec = Some kubernetes.ServiceSpec::spec
      }

in  service