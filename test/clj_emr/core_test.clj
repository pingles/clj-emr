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
               .getBidPrice ""))

;; m1.pingles is not a valid ec2 instance type... yet ;)
(expect AssertionError (instance-group-config :core :instance-type :m1.pingles))

;; need a bid price when using spot market instances
(expect AssertionError (instance-group-config :core :market :spot :bid-price 0))

(given (instance-group-config :core :market :spot :bid-price 0.5)
       (expect .getBidPrice "0.5"))

(given (job-flow-instances :hadoop-version "1.0.3")
       (expect .getHadoopVersion "1.0.3"))

;; invalid hadoop version
(expect AssertionError (job-flow-instances :hadoop-version "2.0"))
