(ns fluree.http-api.system-test
  (:require [clojure.test :refer :all]
            [donut.system :as ds]
            [fluree.http-api.system :as sys]
            [clj-http.client :as http]
            [jsonista.core :as json]
            [clojure.edn :as edn])
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
                           {:method      :memory
                            :parallelism 1
                            :defaults
                            {:context
                             {:id     "@id"
                              :type   "@type"
                              :ex     "http://example.com/"
                              :schema "http://schema.org/"
                              :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}}}}}))

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
        req         (pr-str {:ledger  ledger-name
                             :context {:foo "http://foobar.com/"}
                             :txn     [{:id      :ex/create-test
                                        :type    :foo/test
                                        :ex/name "create-endpoint-test"}]})
        headers     {"Content-Type" "application/edn"
                     "Accept"       "application/edn"}
        res         (update (post :create {:body req :headers headers})
                            :body edn/read-string)]
    (if (= 201 (:status res))
      (get-in res [:body :alias])
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
              :t       -1}
             (-> res :body (json/read-value json/keyword-keys-object-mapper))))))

  (testing "can create a new ledger w/ EDN"
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
              :t       -1}
             (-> res :body edn/read-string)))))

  (testing "responds with 409 error if ledger already exists"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          req         (pr-str {:ledger  ledger-name
                               :context {:foo "http://foobar.com/"}
                               :txn     [{:id      :ex/create-test
                                          :type    :foo/test
                                          :ex/name "create-endpoint-test"}]})
          headers     {"Content-Type" "application/edn"
                       "Accept"       "application/edn"}
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
      (is (= {:address address, :alias ledger-name, :t -2}
             (-> res :body (json/read-value json/keyword-keys-object-mapper))))))

  (testing "can transact in EDN"
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
      (is (= {:address address, :alias ledger-name, :t -2}
             (-> res :body edn/read-string))))))

(deftest ^:integration query-test
  (testing "can query a basic entity"
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
      (is (= 200 (:status query-res)))
      (is (= [{:id       :ex/query-test
               :rdf/type [:schema/Test]
               :ex/name  "query-test"}]
             (-> query-res :body edn/read-string))))))
