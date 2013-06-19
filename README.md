# clj-emr

Clojure library to talk to Amazon's Elastic MapReduce API

[![Build Status](https://travis-ci.org/pingles/clj-emr.png)](https://travis-ci.org/pingles/clj-emr)

## Usage

```clojure
(ns myapp
  (:use [clj-emr.core]))

(def c (client (credentials access-key secret-key) :region :eu-west-1))

(def flow (job-flow "My EMR flow"
                    (job-flow-instances [(instance-group-config :master
                                                                :instance-type :m1.medium
                                                                :market :on-demand)
                                         (instance-group-config :core
                                                                :instance-type :m1.small
                                                                :market :on-demand
                                                                :count 5)
                                         (instance-group-config :task
                                                                :instance-type :cc2.8xlarge
                                                                :count 10
                                                                :market :spot
                                                                :bid-price 0.5)])
                    [(step "First step"
                           (jar-config "s3n://jobs-bucket/my-job.jar"
                                       :main-class "hello.World"
                                       :args ["s3n://input-data/*" "s3n://output-data"]
                                       :on-failure :terminate-flow))]))

(def result (run c flow))
```

## License

Copyright &copy; 2013 Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.
