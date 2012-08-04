;; Author : Naveen Nandan

(ns stream-query-builder.core
	(:gen-class)
	(:import [backtype.storm StormSubmitter LocalCluster])
	(:use [backtype.storm clojure config]))


(def r (new java.util.Random))
(def f (new java.io.File "/Users/naveennandan/Projects/output"))
(def fw (new java.io.FileWriter f))
(def bw (new java.io.BufferedWriter fw))


(defspout data-spout {"data-stream" ["type" "fields"]}
	[conf context collector]
	(spout
		(nextTuple []
			(Thread/sleep 1000)
			(emit-spout! collector ["data" (hash-map "temperature" (.nextInt r 50))]
				:id (str "data" (.nextInt r 10)) :stream "data-stream"))))


(defspout meta-data-spout {"meta-data-stream" ["type" "fields"]}
	[conf context collector]
	(spout
		(nextTuple []
			(Thread/sleep 1000)
			(emit-spout! collector ["meta-data" (hash-map "temperature"
						(.nextInt r 40))]
				:id (str "meta-data" (.nextInt r 10)) :stream "meta-data-stream"))))


(defbolt filter-bolt {"filtered-stream" ["type" "fields"]} {:prepare true}
	[conf context collector]
	(bolt
		(execute [tuple]
			(if (> (get (:fields tuple) "temperature") 35)
				(emit-bolt! collector tuple :anchor tuple
					:stream "filtered-stream")
			)
			(ack! collector tuple))))


(defbolt aggregate-bolt [] {:prepare true}
	[conf context collector]
	(bolt
		(execute [tuple]
			(.write bw (str "stream-type : " (:type tuple)
				"    fields : " (:fields tuple)))
			(.flush bw)
			(.newLine bw)
			(.flush bw)
			(ack! collector tuple))))


(defn mk-topology []
	(topology
		{"data-spout" (spout-spec data-spout)
		 "meta-data-spout" (spout-spec meta-data-spout)}

		{"filter-bolt" (bolt-spec {["data-spout" "data-stream"] :shuffle
					   ["meta-data-spout" "meta-data-stream"] :shuffle}
				filter-bolt)

		 "aggregate-bolt" (bolt-spec {["filter-bolt" "filtered-stream"] :shuffle}
				aggregate-bolt)})
)

(defn run-local! []
	(let [cluster (LocalCluster.)]
                (.submitTopology cluster "my-topology" {TOPOLOGY-DEBUG true} (mk-topology))))


(defn -main []
        (run-local!))
