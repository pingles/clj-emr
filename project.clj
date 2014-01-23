(defproject clj-emr "0.1.10"
  :description "Clojure library to control Amazon Elastic MapReduce"
  :url "http://github.com/pingles/clj-emr"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.amazonaws/aws-java-sdk "1.4.6"]]
  :profiles {:dev {:dependencies [[expectations "1.4.48"]]
                   :plugins [[lein-expectations "0.0.8"]]}})
