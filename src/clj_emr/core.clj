(ns clj-emr.core
  (:import [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.services.elasticmapreduce AmazonElasticMapReduceClient]
           [com.amazonaws.services.elasticmapreduce.model InstanceGroupConfig JobFlowInstancesConfig InstanceRoleType MarketType]))

(def ^{:dynamic true} *instance-types* #{:m1.small
                                         :m1.medium
                                         :m1.large
                                         :m1.xlarge
                                         :m3.xlarge
                                         :m3.2xlarge
                                         :c1.medium
                                         :c1.xlarge
                                         :cc2.8xlarge
                                         :m2.xlarge
                                         :m2.2xlarge
                                         :m2.4xlarge
                                         :cr1.8xlarge
                                         :hi1.4xlarge
                                         :hs1.8xlarge
                                         :t1.micro
                                         :cg1.4xlarge})

(defn credentials
  [access-key secret-key]
  (BasicAWSCredentials. access-key secret-key))

(defn client
  [credentials]
  (AmazonElasticMapReduceClient. credentials))


(defn instance-role
  "role:  #{:core :master}"
  [role]
  (cond (= role :core) (InstanceRoleType/CORE)
        (= role :master) (InstanceRoleType/MASTER)))

(defn market-type
  [type]
  (cond (= type :spot) (MarketType/SPOT)
        (= type :on-demand) (MarketType/ON_DEMAND)))

(defn spot-price-valid?
  [market bid]
  (or (= :on-demand market)
      (and (number? bid)
           (> bid 0))))

(defn valid-instance-type?
  [instance-type]
  (contains? *instance-types* instance-type))

(defn instance-group-config
  "instance-type: :m1.small ... (see *instance-types*)
   market       : #{:on-demand, :spot}
   count        : number of instances
   bid-price    : price in USD per-instance (when in a :spot market)"
  [role & {:keys [instance-type market count bid-price]
           :or   {instance-type :m1.small
                  market        :on-demand
                  count         1}}]
  {:pre [(valid-instance-type? instance-type)
         (spot-price-valid? market bid-price)]}
  (doto (InstanceGroupConfig. )
    (.withInstanceRole (instance-role role))
    (.withInstanceType (name instance-type))
    (.withMarket (market-type market))
    (.withInstanceCount (Integer/valueOf count))
    (.withBidPrice (str bid-price))))

(def ^{:dynamic true} *hadoop-versions* #{"1.0.3" "0.20.205" "0.20" "0.18"})

(defn job-flow-instances
  "key-name      : ec2 keypair name to allow clients to ssh to nodes
   hadoop-version: which hadoop version
   instances     : number of instances in cluster"
  [& {:keys [hadoop-version key-name instances]
      :or   {hadoop-version "1.0.3"}}]
  {:pre [(contains? *hadoop-versions* hadoop-version)]}
  (-> (JobFlowInstancesConfig. )
      (.withEc2KeyName key-name)
      (.withHadoopVersion hadoop-version)))

(defn job-flow
  [name instance-config])
