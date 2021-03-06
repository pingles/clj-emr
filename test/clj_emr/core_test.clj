(ns clj-emr.core-test
  (:use [expectations]
        [clj-emr.core]))

(expect (spot-price-valid? :on-demand nil))
(expect (not (spot-price-valid? :spot nil)))
(expect (spot-price-valid? :spot 1))

(given (instance-group-config :core :market :on-demand)
       (expect .getInstanceRole "CORE"
               .getMarket "ON_DEMAND"
               .getInstanceCount 1
               .getBidPrice nil))

(given (instance-group-config :task :market :spot :bid-price 0.5)
       (expect .getInstanceRole "TASK"
               .getMarket "SPOT"))

;; m1.pingles is not a valid ec2 instance type... yet ;)
(expect AssertionError (instance-group-config :core :instance-type :m1.pingles))

;; need a bid price when using spot market instances
(expect AssertionError (instance-group-config :core :market :spot :bid-price 0))

(given (instance-group-config :core :market :spot :bid-price 0.5)
       (expect .getBidPrice "0.5"))

(given (job-flow-instances [] :hadoop-version "1.0.3")
       (expect .getHadoopVersion "1.0.3"))

;; invalid hadoop version
(expect AssertionError (job-flow-instances [] :hadoop-version "2.0"))


(let [instances (job-flow-instances [(instance-group-config :master :instance-type :m1.medium)
                                     (instance-group-config :core
                                                            :instance-type :m1.small
                                                            :count 20
                                                            :market :spot
                                                            :bid-price 0.5)]
                                    :hadoop-version "1.0.3")]
  (given instances
         (expect #(count (.getInstanceGroups %)) 2)))

(expect (not (.getKeepJobFlowAliveWhenNoSteps (job-flow-instances [] :keep-alive? false))))

(given (job-flow-instances [] :keep-alive? false)
       (expect .getKeepJobFlowAliveWhenNoSteps false))


(given (job-flow "test flow"
                 (job-flow-instances [])
                 []
                 :log-uri "bucket/test-path")
       (expect .getName "test flow"
               .getLogUri "bucket/test-path"
               .getAmiVersion "2.3"
               .getVisibleToAllUsers false))

(expect AssertionError (job-flow "testing" (job-flow-instances []) [] :ami-version "pingles"))


;; jar setup
(given (jar-config "bucket/my.jar")
       (expect .getJar "bucket/my.jar"
               .getArgs empty?
               .getProperties empty?))

(expect AssertionError (jar-config "some.jar" :args "needs to be a vector"))

(given (jar-config "bucket/my.jar" :args ["-Xmx1G"] :properties {"some.prop" "val"
                                                                 "another.prop" "val"})
       (expect .getArgs ["-Xmx1G"]
               #(count (.getProperties %)) 2))

(given (jar-config "bucket/my.jar" :main-class "some.Class")
       (expect .getMainClass "some.Class"))

(given (jar-config "bucket/my.jar" :main-class Object)
       (expect .getMainClass "java.lang.Object"))


(given (step "first step" (jar-config "bucket/my.jar" :main-class "hello.World"))
       (expect .getName "first step"
               .getActionOnFailure "TERMINATE_JOB_FLOW"))

(given (step "wait" (jar-config "some.jar") :on-failure :cancel-and-wait)
       (expect .getActionOnFailure "CANCEL_AND_WAIT"))

(expect AssertionError (step "blah" (jar-config "some.jar") :on-failure :bad-value))

(given (job-flow "Sample flow"
                 (job-flow-instances [(instance-group-config :master
                                                             :instance-type :m1.medium
                                                             :market :on-demand)
                                      (instance-group-config :core
                                                             :instance-type :m1.small
                                                             :market :on-demand
                                                             :count 5)
                                      (instance-group-config :task
                                                             :instance-type :m1.small
                                                             :count 20
                                                             :market :spot
                                                             :bid-price 0.5)]
                                     :hadoop-version "1.0.3")
                 [(step "First step"
                        (jar-config "bucket/my.jar"
                                    :main-class "hello.World"
                                    :args ["-input" "/path" "-output" "-path"]
                                    :on-failure :cancel-and-wait))])
       (expect .getName "Sample flow"
               #(count (.getSteps %)) 1))


(expect AssertionError (client (credentials "" "") :region :pingles))
(expect (client (credentials "" "") :region :eu-west-1))
