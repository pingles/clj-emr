(ns clj-emr.core
  (:import [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.services.elasticmapreduce AmazonElasticMapReduceClient]
           [com.amazonaws.services.elasticmapreduce.model RunJobFlowRequest InstanceGroupConfig JobFlowInstancesConfig InstanceRoleType MarketType StepConfig HadoopJarStepConfig KeyValue ActionOnFailure]))

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

(def ^{:dynamic true} *endpoints* {:us-east-1 "elasticmapreduce.us-east-1.amazonaws.com"
                                   :us-west-2 "elasticmapreduce.us-west-2.amazonaws.com"
                                   :us-west-1 "elasticmapreduce.us-west-1.amazonaws.com"
                                   :eu-west-1 "elasticmapreduce.eu-west-1.amazonaws.com"
                                   :ap-southeast-1 "elasticmapreduce.ap-southeast-1.amazonaws.com"
                                   :ap-southeast-2 "elasticmapreduce.ap-southeast-2.amazonaws.com"
                                   :ap-northeast-1 "elasticmapreduce.ap-northeast-1.amazonaws.com"
                                   :sa-east-1 "elasticmapreduce.sa-east-1.amazonaws.com"})

(defn client
  [credentials & {:keys [region]
                  :or   {region :us-east-1}}]
  {:pre [(contains? *endpoints* region)]}
  (doto (AmazonElasticMapReduceClient. credentials)
    (.setEndpoint (region *endpoints*))))

(def instance-role {:core (InstanceRoleType/CORE)
                    :master (InstanceRoleType/MASTER)
                    :task (InstanceRoleType/TASK)})

(def market-type {:spot (MarketType/SPOT)
                  :on-demand (MarketType/ON_DEMAND)})

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
    (.withBidPrice (when bid-price
                     (str bid-price)))))

(def ^{:dynamic true} *hadoop-versions* #{"1.0.3" "0.20.205" "0.20" "0.18"})

(defn job-flow-instances
  "key-name      : ec2 keypair name to allow clients to ssh to nodes
   hadoop-version: which hadoop version
   master        : config map for master role. see instance-group-config
   core          : config map for core nodes. see instance-group-config
   task          : config for task nodes"
  [instances & {:keys [hadoop-version key-name keep-alive?]
                :or   {hadoop-version "1.0.3"
                       keep-alive?     false}}]
  {:pre [(contains? *hadoop-versions* hadoop-version)]}
  (-> (JobFlowInstancesConfig. )
      (.withEc2KeyName key-name)
      (.withHadoopVersion hadoop-version)
      (.withKeepJobFlowAliveWhenNoSteps keep-alive?)
      (.withInstanceGroups instances)))

(defn- key-vals
  "Converts an associative structure to a collection of KeyValue"
  [m]
  (map (fn [[key val]] (KeyValue. key val))
       m))

(defmulti class-name class)
(defmethod class-name nil [_])
(defmethod class-name String [x] x)
(defmethod class-name Class [x] (.getName x))

(defn jar-config
  [jar-path & {:keys [args main-class properties]}]
  (doto (HadoopJarStepConfig. jar-path)
    (.withArgs args)
    (.withProperties (key-vals properties))
    (.withMainClass (class-name main-class))))

(def action-on-failure {:cancel-and-wait (ActionOnFailure/CANCEL_AND_WAIT)
                        :continue (ActionOnFailure/CONTINUE)
                        :terminate-flow (ActionOnFailure/TERMINATE_JOB_FLOW)})

(defn step
  "name      : name for the step
   jar-config: job jar config, see jar-config."
  [name jar-config & {:keys [on-failure]
                      :or   {on-failure :terminate-flow}}]
  (doto (StepConfig. name jar-config)
    (.withActionOnFailure (action-on-failure on-failure))))

(def ^{:dynamic true} *ami-versions* #{"2.3" "2.2" "2.1" "2.0" "1.0"})

(defn job-flow
  "name     : name the job
   instances: job flow instance config, see job-flow-instances"
  [name instances steps & {:keys [log-uri ami-version visible-to-all?]
                           :or   {ami-version "2.3"
                                  visible-to-all? false}}]
  {:pre [(contains? *ami-versions* ami-version)]}
  (doto (RunJobFlowRequest. name instances)
    (.withLogUri log-uri)
    (.withAmiVersion ami-version)
    (.withVisibleToAllUsers visible-to-all?)
    (.withSteps steps)))



(defn run
  "Creates and starts running the job flow"
  [client job-flow]
  (.runJobFlow client job-flow))
