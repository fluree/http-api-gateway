(ns fluree.http-api.integration.multi-query-test
  (:require [clojure.test :refer :all]
            [fluree.http-api.integration.test-system :refer :all]
            [jsonista.core :as json]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:json multi-query-json-test
  (testing "can run a multi-query w/ opts in JSON"
    (let [ledger-name  (create-rand-ledger "multi-query-opts-json-test")
          json-headers {"Content-Type" "application/json"
                        "Accept"       "application/json"}
          wes-did      "did:fluree:Tf6i5oh2ssYNRpxxUM2zea1Yo7x4uRqyTeU"
          txn-req      {:body
                        (json/write-value-as-string
                         {"ledger" ledger-name
                          "txn"    [{"id"      "ex:query-test"
                                     "type"    "schema:Test"
                                     "ex:name" "query-test"}
                                    {"id"       "ex:wes"
                                     "type"     "ex:User"
                                     "ex:fname" "Wes"}
                                    {"id"      "ex:iphone"
                                     "type"    "schema:Device"
                                     "ex:name" "iPhone"}
                                    {"id"            "ex:UserPolicy"
                                     "type"          ["f:Policy"]
                                     "f:targetClass" [{"id" "schema:Test"}
                                                      {"id" "ex:User"}
                                                      {"id" "schema:Device"}]
                                     "f:allow"
                                     [{"id"           "ex:globalViewAllow"
                                       "f:targetRole" {"id" "ex:userRole"}
                                       "f:action"     [{"id" "f:view"}]}]}
                                    {"id"      wes-did
                                     "ex:User" {"id" "ex:wes"}
                                     "f:role"  {"id" "ex:userRole"}}]})
                        :headers json-headers}
          txn-res      (post :transact txn-req)
          _            (assert (= 200 (:status txn-res)))
          query-req    {:body
                        (json/write-value-as-string
                         {"ledger" ledger-name
                          "query"  {"test"
                                    {"select" '{?t ["*"]}
                                     "where"  '[[?t "type" "schema:Test"]]}
                                    "person"
                                    {"select" '{?p ["*"]}
                                     "where"  '[[?p "type" "ex:User"]]}
                                    "device"
                                    {"select" '{?d ["*"]}
                                     "where"  '[[?d "type" "schema:Device"]]}
                                    "opts" {"role" "ex:userRole"
                                            "did"  wes-did}}})
                        :headers json-headers}
          query-res    (post :multi-query query-req)]
      (is (= 200 (:status query-res))
          (str "Query result was: " (pr-str query-res)))
      (is (= {"device" [{"ex:name"  "iPhone"
                         "id"       "ex:iphone"
                         "rdf:type" ["schema:Device"]}],
              "person" [{"ex:fname" "Wes"
                         "id"       "ex:wes"
                         "rdf:type" ["ex:User"]}],
              "test"   [{"ex:name"  "query-test"
                         "id"       "ex:query-test"
                         "rdf:type" ["schema:Test"]}]}
             (-> query-res :body json/read-value))))))
