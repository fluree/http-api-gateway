(ns fluree.http-api.integration.history-query-test
  (:require [clojure.test :refer :all]
            [fluree.http-api.integration.test-system :refer :all]
            [jsonista.core :as json]
            [clojure.edn :as edn]))

(use-fixtures :once run-test-server)

(def commit-id-regex
  (re-pattern "fluree:commit:sha256:[a-z2-7]{52,53}"))

(def mem-addr-regex
  (re-pattern "fluree:memory://[a-f0-9]{64}"))

(def context-id-regex
  (re-pattern "fluree:context:[a-f0-9]{64}"))

(def db-id-regex
  (re-pattern "fluree:db:sha256:[a-z2-7]{52,53}"))

(deftest ^:integration ^:json history-query-json-test
  (testing "basic JSON history query works"
    (let [ledger-name   "history-query-json-test"
          txn-req       {:body
                         (json/write-value-as-string
                          {"f:ledger" ledger-name
                           "@graph"   [{"id"      "ex:query-test"
                                        "type"    "schema:Test"
                                        "ex:name" "query-test"}]})
                         :headers json-headers}
          txn-res       (api-post :create txn-req)
          _             (assert (= 201 (:status txn-res)))
          txn2-req      {:body
                         (json/write-value-as-string
                           {"f:ledger" ledger-name
                            "@graph"   [{"id"           "ex:query-test"
                                         "ex:test-type" "integration"}]})
                         :headers json-headers}
          txn2-res      (api-post :transact txn2-req)
          _             (assert (= 200 (:status txn2-res)))
          query-req     {:body
                         (json/write-value-as-string
                           {"from"           ledger-name
                            "commit-details" true
                            "t"              {"at" "latest"}})
                         :headers json-headers}
          query-res     (api-post :history query-req)
          query-results (-> query-res :body json/read-value)]
      (is (= 200 (:status query-res))
          (str "History query response was: " (pr-str query-res)))
      (let [history-expectations
            {["id"]                  #(re-matches commit-id-regex %)
             ["f:address"]           #(re-matches mem-addr-regex %)
             ["f:alias"]             #(= % ledger-name)
             ["f:branch"]            #(= % "main")
             ["f:defaultContext"]    #(re-matches context-id-regex (get % "id"))
             ["f:previous"]          #(or (nil? %)
                                          (re-matches commit-id-regex (get % "id")))
             ["f:time"]              pos-int?
             ["f:v"]                 nat-int?
             ["f:data" "f:address"]  #(re-matches mem-addr-regex %)
             ["f:data" "f:assert"]   #(and (vector? %) (every? map? %))
             ["f:data" "f:retract"]  #(and (vector? %) (every? map? %))
             ["f:data" "f:flakes"]   pos-int?
             ["f:data" "f:size"]     pos-int?
             ["f:data" "f:t"]        pos-int?
             ["f:data" "f:previous"] #(or (nil? %)
                                          (re-matches db-id-regex (get % "id")))}]
        (is (every? #(every? (fn [[kp valid?]]
                               (let [actual (get-in % kp)]
                                 (or
                                  (valid? actual)
                                  (println "invalid:" (pr-str actual)))))
                             history-expectations)
                    (map #(get % "f:commit") query-results)))))))


(deftest ^:integration ^:edn history-query-edn-test
  (testing "basic EDN history query works"
    (let [ledger-name   "history-query-edn-test"
          txn-req       {:body
                         (pr-str
                          {:f/ledger ledger-name
                           :graph    [{:id      :ex/query-test
                                       :type    :schema/Test
                                       :ex/name "query-test"}]})
                         :headers edn-headers}
          txn-res       (api-post :create txn-req)
          _             (assert (= 201 (:status txn-res)))
          txn2-req      {:body
                         (pr-str
                           {:context  ["" {:id "@id", :graph "@graph"}]
                            :f/ledger ledger-name
                            :graph    [{:id           :ex/query-test
                                        :ex/test-type "integration"}]})
                         :headers edn-headers}
          txn2-res      (api-post :transact txn2-req)
          _             (assert (= 200 (:status txn2-res)))
          query-req     {:body
                         (pr-str
                           {:from           ledger-name
                            :commit-details true
                            :t              {:from 1}})
                         :headers edn-headers}
          query-res     (api-post :history query-req)
          query-results (-> query-res :body edn/read-string)]
      (is (= 200 (:status query-res))
          (str "History query response was: " (pr-str query-res)))
      (let [history-expectations
            {[:id]                 #(re-matches commit-id-regex %)
             [:f/address]          #(re-matches mem-addr-regex %)
             [:f/alias]            #(= % ledger-name)
             [:f/branch]           #(= % "main")
             [:f/defaultContext]   #(re-matches context-id-regex (:id %))
             [:f/previous]         #(or (nil? %)
                                        (re-matches commit-id-regex (:id %)))
             [:f/time]             pos-int?
             [:f/v]                nat-int?
             [:f/data :f/address]  #(re-matches mem-addr-regex %)
             [:f/data :f/assert]   #(and (vector? %) (every? map? %))
             [:f/data :f/retract]  #(and (vector? %) (every? map? %))
             [:f/data :f/flakes]   pos-int?
             [:f/data :f/size]     pos-int?
             [:f/data :f/t]        pos-int?
             [:f/data :f/previous] #(or (nil? %)
                                        (re-matches db-id-regex (:id %)))}]
        (is (every? #(every? (fn [[kp valid?]]
                               (let [actual (get-in % kp)]
                                 (or
                                  (valid? actual)
                                  (println "invalid:" (pr-str actual)))))
                             history-expectations)
                    (map :f/commit query-results)))))))
