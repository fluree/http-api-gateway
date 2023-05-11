(ns fluree.http-api.integration.basic-query-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [fluree.http-api.integration.test-system :refer :all]
            [jsonista.core :as json]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:json query-json-test
  (testing "can query a basic entity w/ JSON"
    (let [ledger-name  (create-rand-ledger "query-endpoint-basic-entity-test")
          json-headers {"Content-Type" "application/json"
                        "Accept"       "application/json"}
          txn-req      {:body
                        (json/write-value-as-string
                         {"ledger" ledger-name
                          "txn"    [{"id"      "ex:query-test"
                                     "type"    "schema:Test"
                                     "ex:name" "query-test"}]})
                        :headers json-headers}
          txn-res      (post :transact txn-req)
          _            (assert (= 200 (:status txn-res)))
          query-req    {:body
                        (json/write-value-as-string
                         {"ledger" ledger-name
                          "query"  {"select" '{?t ["*"]}
                                    "where"  '[[?t "type" "schema:Test"]]}})
                        :headers json-headers}
          query-res    (post :query query-req)]
      (is (= 200 (:status query-res)))
      (is (= [{"id"       "ex:query-test"
               "rdf:type" ["schema:Test"]
               "ex:name"  "query-test"}]
             (-> query-res :body json/read-value)))))

  (testing "union query works"
    (let [ledger-name  (create-rand-ledger "query-endpoint-union-test")
          json-headers {"Content-Type" "application/json"
                        "Accept"       "application/json"}
          txn-req      {:body
                        (json/write-value-as-string
                         {"ledger" ledger-name
                          "txn"    [{"id"      "ex:query-test"
                                     "type"    "schema:Test"
                                     "ex:name" "query-test"}
                                    {"id"       "ex:wes"
                                     "type"     "schema:Person"
                                     "ex:fname" "Wes"}]})
                        :headers json-headers}
          txn-res      (post :transact txn-req)
          _            (assert (= 200 (:status txn-res)))
          query-req    {:body
                        (json/write-value-as-string
                         {"ledger" ledger-name
                          "query"  '{"select" ?n
                                     "where"  [{"union"
                                                [[[?s "ex:name" ?n]]
                                                 [[?s "ex:fname" ?n]]]}]}})
                        :headers json-headers}
          query-res    (post :query query-req)]
      (is (= 200 (:status query-res)))
      (is (= ["query-test" "Wes"]
             (-> query-res :body json/read-value)))))

  (testing "optional query works"
    (let [ledger-name  (create-rand-ledger "query-endpoint-optional-test")
          json-headers {"Content-Type" "application/json"
                        "Accept"       "application/json"}
          txn-req      {:body
                        (json/write-value-as-string
                         {"ledger" ledger-name
                          "txn"    [{"id"          "ex:brian",
                                     "type"        "ex:User",
                                     "schema:name" "Brian"
                                     "ex:friend"   [{"id" "ex:alice"}]}
                                    {"id"           "ex:alice",
                                     "type"         "ex:User",
                                     "ex:favColor"  "Green"
                                     "schema:email" "alice@flur.ee"
                                     "schema:name"  "Alice"}
                                    {"id"           "ex:cam",
                                     "type"         "ex:User",
                                     "schema:name"  "Cam"
                                     "schema:email" "cam@flur.ee"
                                     "ex:friend"    [{"id" "ex:brian"}
                                                     {"id" "ex:alice"}]}]})
                        :headers json-headers}
          txn-res      (post :transact txn-req)
          _            (assert (= 200 (:status txn-res)))
          query        {"ledger" ledger-name
                        "query"  '{"select" [?name ?favColor]
                                   "where"  [[?s "rdf:type" "ex:User"]
                                             [?s "schema:name" ?name]
                                             {"optional" [?s "ex:favColor" ?favColor]}]}}
          query-req    {:body
                        (json/write-value-as-string query)
                        :headers json-headers}
          query-res    (post :query query-req)]
      (is (= 200 (:status query-res))
          (str "Response was: " (pr-str query-res)))
      (is (= [["Cam" nil]
              ["Alice" "Green"]
              ["Brian" nil]]
             (-> query-res :body json/read-value))
          (str "Response was: " (pr-str query-res)))))

  (testing "selectOne query works"
    (let [ledger-name  (create-rand-ledger "query-endpoint-basic-entity-test")
          json-headers {"Content-Type" "application/json"
                        "Accept"       "application/json"}
          txn-req      {:body
                        (json/write-value-as-string
                         {"ledger" ledger-name
                          "txn"    [{"id"      "ex:query-test"
                                     "type"    "schema:Test"
                                     "ex:name" "query-test"}]})
                        :headers json-headers}
          txn-res      (post :transact txn-req)
          _            (assert (= 200 (:status txn-res)))
          query-req    {:body
                        (json/write-value-as-string
                         {"ledger" ledger-name
                          "query"  {"selectOne" '{?t ["*"]}
                                    "where"     '[[?t "type" "schema:Test"]]}})
                        :headers json-headers}
          query-res    (post :query query-req)]
      (is (= 200 (:status query-res)))
      (is (= {"id"       "ex:query-test"
              "rdf:type" ["schema:Test"]
              "ex:name"  "query-test"}
             (-> query-res :body json/read-value))))))

(deftest ^:integration ^:edn query-edn-test
  (testing "can query a basic entity w/ EDN"
    (let [ledger-name (create-rand-ledger "query-endpoint-basic-entity-test")
          edn-headers {"Content-Type" "application/edn"
                       "Accept"       "application/edn"}
          txn-req     {:body
                       (pr-str {:ledger ledger-name
                                :txn    [{:id      :ex/query-test
                                          :type    :schema/Test
                                          :ex/name "query-test"}]})
                       :headers edn-headers}
          txn-res     (post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))
          query-req   {:body
                       (pr-str {:ledger ledger-name
                                :query  {:select '{?t [:*]}
                                         :where  '[[?t :type :schema/Test]]}})
                       :headers edn-headers}
          query-res   (post :query query-req)]
      (is (= 200 (:status query-res))
          (str "Query response was:" (pr-str query-res)))
      (is (= [{:id       :ex/query-test
               :rdf/type [:schema/Test]
               :ex/name  "query-test"}]
             (-> query-res :body edn/read-string))))))
