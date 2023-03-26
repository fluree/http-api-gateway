(ns fluree.http-api.system-test
  (:require [clojure.test :refer :all]
            [donut.system :as ds]
            [fluree.http-api.system :as sys]
            [clj-http.client :as http]
            [jsonista.core :as json]
            [clojure.edn :as edn]
            [fluree.db.util.log :as log])
  (:import (java.net ServerSocket)))

(defn find-open-port
  ([] (find-open-port nil))
  ([_] ; so it can be used in swap!
   (let [socket (ServerSocket. 0)]
     (.close socket)
     (.getLocalPort socket))))

(defonce api-port (atom nil))

(defmethod ds/named-system :test
  [_]
  (ds/system :dev {[:env] {:http/server {:port @api-port}
                           :fluree/connection
                           {"method"      "memory"
                            "parallelism" 1
                            "defaults"
                            {"@context"
                             {"id"     "@id"
                              "type"   "@type"
                              "ex"     "http://example.com/"
                              "schema" "http://schema.org/"
                              "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                              "f"      "https://ns.flur.ee/ledger#"}}}}}))

(defn run-test-server
  [run-tests]
  (swap! api-port find-open-port)
  (let [stop-server (sys/run-server {:profile :test})]
    (run-tests)
    (stop-server)))

(use-fixtures :once run-test-server)

(defn api-url [endpoint]
  (str "http://localhost:" @api-port "/fluree/" (name endpoint)))

(defn post [endpoint req]
  (http/post (api-url endpoint) (assoc req :throw-exceptions false)))

(defn create-rand-ledger
  [name-root]
  (let [ledger-name (str name-root "-" (random-uuid))
        req         (json/write-value-as-string
                      {:ledger  ledger-name
                       :context {:foo "http://foobar.com/"}
                       :txn     [{:id      :ex/create-test
                                  :type    :foo/test
                                  :ex/name "create-endpoint-test"}]})
        headers     {"Content-Type" "application/json"
                     "Accept"       "application/json"}
        res         (update (post :create {:body req :headers headers})
                            :body json/read-value)]
    (if (= 201 (:status res))
      (get-in res [:body "alias"])
      (throw (ex-info "Error creating random ledger" res)))))

(deftest ^:integration create-endpoint-test
  (testing "can create a new ledger w/ JSON"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (json/write-value-as-string
                        {:ledger  ledger-name
                         :context {:foo "http://foobar.com/"}
                         :txn     [{:id      :ex/create-test
                                    :type    :foo/test
                                    :ex/name "create-endpoint-test"}]})
          headers     {"Content-Type" "application/json"
                       "Accept"       "application/json"}
          res         (post :create {:body req :headers headers})]
      (is (= 201 (:status res)))
      (is (= {:address address
              :alias   ledger-name
              :t       1}
             (-> res :body (json/read-value json/keyword-keys-object-mapper))))))

  ;; TODO: Re-enable when EDN works again
  #_(testing "can create a new ledger w/ EDN"
      (let [ledger-name (str "create-endpoint-" (random-uuid))
            address     (str "fluree:memory://" ledger-name "/main/head")
            req         (pr-str {:ledger  ledger-name
                                 :context {:foo "http://foobar.com/"}
                                 :txn     [{:id      :ex/create-test
                                            :type    :foo/test
                                            :ex/name "create-endpoint-test"}]})
            headers     {"Content-Type" "application/edn"
                         "Accept"       "application/edn"}
            res         (post :create {:body req :headers headers})]
        (is (= 201 (:status res)))
        (is (= {:address address
                :alias   ledger-name
                :t       1}
               (-> res :body edn/read-string)))))

  (testing "responds with 409 error if ledger already exists"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          req         (json/write-value-as-string
                        {"ledger"   ledger-name
                         "@context" {"foo" "http://foobar.com/"}
                         "txn"      [{"id"      "ex:create-test"
                                      "type"    "foo:test"
                                      "ex:name" "create-endpoint-test"}]})
          headers     {"Content-Type" "application/json"
                       "Accept"       "application/json"}
          res-success (post :create {:body req :headers headers})
          _           (assert (= 201 (:status res-success)))
          res-fail    (post :create {:body req :headers headers})]
      (is (= 409 (:status res-fail))))))

(deftest ^:integration transaction-test
  (testing "can transact in JSON"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (json/write-value-as-string
                        {:ledger ledger-name
                         :txn    {:id      :ex/transaction-test
                                  :type    :schema/Test
                                  :ex/name "transact-endpoint-json-test"}})
          headers     {"Content-Type" "application/json"
                       "Accept"       "application/json"}
          res         (post :transact {:body req :headers headers})]
      (is (= 200 (:status res)))
      (is (= {:address address, :alias ledger-name, :t 2}
             (-> res :body (json/read-value json/keyword-keys-object-mapper))))))

  ;; TODO: Re-enable when EDN works again
  #_(testing "can transact in EDN"
      (let [ledger-name (create-rand-ledger "transact-endpoint-edn-test")
            address     (str "fluree:memory://" ledger-name "/main/head")
            req         (pr-str
                          {:ledger ledger-name
                           :txn    [{:id      :ex/transaction-test
                                     :type    :schema/Test
                                     :ex/name "transact-endpoint-edn-test"}]})
            headers     {"Content-Type" "application/edn"
                         "Accept"       "application/edn"}
            res         (post :transact {:body req :headers headers})]
        (is (= 200 (:status res)))
        (is (= {:address address, :alias ledger-name, :t 2}
               (-> res :body edn/read-string))))))

(deftest ^:integration query-test
  (testing "can query a basic entity"
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
             (-> query-res :body json/read-value))))))

(deftest ^:integration history-query-test
  (testing "basic JSON subject history query works"
    (let [ledger-name  (create-rand-ledger "history-query-basic-test")
          json-headers {"Content-Type" "application/json"
                        "Accept"       "application/json"}
          txn1-req     {:body
                        (json/write-value-as-string
                          {"ledger" ledger-name
                           "txn"    [{"id"      "ex:query-test"
                                      "type"    "schema:Test"
                                      "ex:name" "query-test"}]})
                        :headers json-headers}
          txn1-res     (post :transact txn1-req)
          _            (assert (= 200 (:status txn1-res)))
          txn2-req     {:body
                        (json/write-value-as-string
                          {"ledger" ledger-name
                           "txn"    [{"id"      "ex:query-test"
                                      "ex:city" "Chicago"}]})
                        :headers json-headers}
          txn2-res     (post :transact txn2-req)
          _            (assert (= 200 (:status txn2-res)))
          query-req    {:body
                        (json/write-value-as-string
                          {"ledger" ledger-name
                           "query"  {"history" "ex:query-test"
                                     "t"       {"from" 1}}})
                        :headers json-headers}
          query-res    (post :history query-req)]
      (log/debug "History query response was:" query-res)
      (is (= 200 (:status query-res))
          (str "History query response was: " (pr-str query-res)))
      (is (= [{"f:t"       2
               "f:assert"  [{"id"       "ex:query-test"
                             "rdf:type" ["schema:Test"]
                             "ex:name"  "query-test"}]
               "f:retract" []}
              {"f:t"       3
               "f:assert"  [{"id"      "ex:query-test"
                             "ex:city" "Chicago"}]
               "f:retract" []}]
             (-> query-res :body json/read-value)))))

  (testing "basic JSON subject history query w/ :at :latest works"
    (let [ledger-name  (create-rand-ledger "history-query-basic-test")
          json-headers {"Content-Type" "application/json"
                        "Accept"       "application/json"}
          txn1-req     {:body
                        (json/write-value-as-string
                          {"ledger" ledger-name
                           "txn"    [{"id"      "ex:query-test"
                                      "type"    "schema:Test"
                                      "ex:name" "query-test"}]})
                        :headers json-headers}
          txn1-res     (post :transact txn1-req)
          _            (assert (= 200 (:status txn1-res)))
          txn2-req     {:body
                        (json/write-value-as-string
                          {"ledger" ledger-name
                           "txn"    [{"id"      "ex:query-test"
                                      "ex:city" "Chicago"}]})
                        :headers json-headers}
          txn2-res     (post :transact txn2-req)
          _            (assert (= 200 (:status txn2-res)))
          query-req    {:body
                        (json/write-value-as-string
                          {"ledger" ledger-name
                           "query"  {"history" "ex:query-test"
                                     "t"       {"at" "latest"}}})
                        :headers json-headers}
          query-res    (post :history query-req)]
      (is (= 200 (:status query-res))
          (str "History query response was: " (pr-str query-res)))
      (is (= [{"f:t"       3
               "f:assert"  [{"id"      "ex:query-test"
                             "ex:city" "Chicago"}]
               "f:retract" []}]
             (-> query-res :body json/read-value)))))

  ;; TODO: Make this pass
  #_(testing "commit-details JSON history query works"
      (let [ledger-name  (create-rand-ledger "history-query-basic-test")
            json-headers {"Content-Type" "application/json"
                          "Accept"       "application/json"}
            txn1-req     {:body
                          (json/write-value-as-string
                            {:ledger ledger-name
                             :txn    [{"id"      "ex:query-test"
                                       "type"    "schema:Test"
                                       "ex:name" "query-test"}]})
                          :headers json-headers}
            txn1-res     (post :transact txn1-req)
            _            (assert (= 200 (:status txn1-res)))
            txn2-req     {:body
                          (json/write-value-as-string
                            {:ledger ledger-name
                             :txn    [{"id"      "ex:query-test"
                                       "ex:city" "Chicago"}]})
                          :headers json-headers}
            txn2-res     (post :transact txn2-req)
            _            (assert (= 200 (:status txn2-res)))
            query-req    {:body
                          (json/write-value-as-string
                            {"ledger" ledger-name
                             "query"  {"commit-details" true
                                       "t"              {"from" 2}}})
                          :headers json-headers}
            query-res    (post :history query-req)]
        (is (= 200 (:status query-res))
            (str "History query response was: " (pr-str query-res)))
        (is (= [{"f:commit"
                 {"f:address" (str "fluree:memory://" ledger-name)
                  ;; TODO: Add the rest of the commit-details keys
                  "f:data"
                  {"f:assert"
                   [{"id"       "ex:query-test"
                     "rdf:type" ["schema:Test"]
                     "ex:name"  "query-test"}]
                   "f:retract" []}}}
                {"f:commit"
                 {"f:data"
                  {"f:assert"
                   [{"id"      "ex:query-test"
                     "ex:city" "Chicago"}]
                   "f:retract" []}}}]
               (-> query-res :body json/read-value))))))
