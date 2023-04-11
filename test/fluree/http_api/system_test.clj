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
                     {"ledger"   ledger-name
                      "defaults" {"@context" ["" {"foo" "http://foobar.com/"}]}
                      "txn"      [{"id"       "foo:create-test"
                                   "type"     "foo:test"
                                   "foo:name" "create-endpoint-test"}]})
        headers     {"Content-Type" "application/json"
                     "Accept"       "application/json"}
        res         (update (post :create {:body req :headers headers})
                            :body json/read-value)]
    (if (= 201 (:status res))
      (get-in res [:body "alias"])
      (throw (ex-info "Error creating random ledger" res)))))

(deftest ^:integration ^:json create-endpoint-json-test
  (testing "can create a new ledger w/ JSON"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (json/write-value-as-string
                       {"ledger"   ledger-name
                        "defaults" {"@context"
                                    ["" {"foo" "http://foobar.com/"}]}
                        "txn"      [{"id"      "ex:create-test"
                                     "type"    "foo:test"
                                     "ex:name" "create-endpoint-test"}]})
          headers     {"Content-Type" "application/json"
                       "Accept"       "application/json"}
          res         (post :create {:body req :headers headers})]
      (is (= 201 (:status res)))
      (is (= {"address" address
              "alias"   ledger-name
              "t"       1}
             (-> res :body json/read-value)))))

  (testing "responds with 409 error if ledger already exists"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          req         (json/write-value-as-string
                       {"ledger"   ledger-name
                        "defaults" {"@context" ["" {"foo" "http://foobar.com/"}]}
                        "txn"      [{"id"      "ex:create-test"
                                     "type"    "foo:test"
                                     "ex:name" "create-endpoint-test"}]})
          headers     {"Content-Type" "application/json"
                       "Accept"       "application/json"}
          res-success (post :create {:body req :headers headers})
          _           (assert (= 201 (:status res-success)))
          res-fail    (post :create {:body req :headers headers})]
      (is (= 409 (:status res-fail))))))

(deftest ^:integration ^:edn create-endpoint-edn-test
  (testing "can create a new ledger w/ EDN"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (pr-str {:ledger   ledger-name
                               :defaults {:context {:foo "http://foobar.com/"}}
                               :txn      [{:id      :ex/create-test
                                           :type    :foo/test
                                           :ex/name "create-endpoint-test"}]})
          headers     {"Content-Type" "application/edn"
                       "Accept"       "application/edn"}
          res         (post :create {:body req :headers headers})]
      (is (= 201 (:status res)))
      (is (= {:address address
              :alias   ledger-name
              :t       1}
             (-> res :body edn/read-string))))))

(deftest ^:integration ^:json transaction-json-test
  (testing "can transact in JSON"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (json/write-value-as-string
                       {"ledger" ledger-name
                        "txn"    {"id"      "ex:transaction-test"
                                  "type"    "schema:Test"
                                  "ex:name" "transact-endpoint-json-test"}})
          headers     {"Content-Type" "application/json"
                       "Accept"       "application/json"}
          res         (post :transact {:body req :headers headers})]
      (is (= 200 (:status res)))
      (is (= {"address" address, "alias" ledger-name, "t" 2}
             (-> res :body json/read-value))))))

(deftest ^:integration ^:edn transaction-edn-test
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
      (is (= {:address address, :alias ledger-name, :t 2}
             (-> res :body edn/read-string))))))

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
      (is (= 200 (:status query-res)))
      (is (= [{:id       :ex/query-test
               :rdf/type [:schema/Test]
               :ex/name  "query-test"}]
             (-> query-res :body edn/read-string))))))

(deftest ^:integration ^:json history-query-json-test
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

  ;; TODO: Get the right expectations in here & make this pass
  ;;       Needs https://github.com/fluree/db/issues/451 fixed
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

(deftest ^:integration ^:edn history-query-edn-test
  (testing "basic EDN history query works"
    (let [ledger-name (create-rand-ledger "history-query-basic-test")
          edn-headers {"Content-Type" "application/edn"
                       "Accept"       "application/edn"}
          txn-req     {:body
                       (pr-str
                        {:ledger ledger-name
                         :txn    [{:id      :ex/query-test
                                   :type    :schema/Test
                                   :ex/name "query-test"}]})
                       :headers edn-headers}
          txn-res     (post :transact txn-req)
          _           (assert (= 200 (:status txn-res)))
          query-req   {:body
                       (pr-str
                        {:ledger ledger-name
                         :query  {:commit-details true
                                  :t              {:at "latest"}}})
                       :headers edn-headers}
          query-res   (post :history query-req)]
      (is (= 200 (:status query-res))
          (str "History query response was: " (pr-str query-res)))
      (is (= [{:f/commit
               {:f/data
                {:f/assert
                 [{:ex/name  "query-test"
                   :id       :ex/query-test
                   :rdf/type [:schema/Test]}]
                 :f/retract []}}}]
             (-> query-res :body edn/read-string))))))


(deftest ^:integration ^:json policy-opts-test
  (testing "policy-enforcing opts are correctly handled"
    (let [ledger-name  (create-rand-ledger "policy-opts-test")
          json-headers {"Content-Type" "application/json"
                        "Accept"       "application/json"}
          alice-did    "did:fluree:Tf6i5oh2ssYNRpxxUM2zea1Yo7x4uRqyTeU"
          txn-req      {:body
                        (json/write-value-as-string
                         {:ledger ledger-name
                          :txn    [{"id"        "ex:alice"
                                    "type"      "ex:User"
                                    "ex:secret" "alice's secret"}
                                   {"id"        "ex:bob"
                                    "type"      "ex:User"
                                    "ex:secret" "bob's secret"}
                                   {"id"            "ex:UserPolicy"
                                    "type"          ["f:Policy"]
                                    "f:targetClass" {"id" "ex:User"}
                                    "f:allow"
                                    [{"id"           "ex:globalViewAllow"
                                      "f:targetRole" {"id" "ex:userRole"}
                                      "f:action"     [{"id" "f:view"}]}]
                                    "f:property"
                                    [{"f:path" {"id" "ex:secret"}
                                      "f:allow"
                                      [{"id"           "ex:secretsRule"
                                        "f:targetRole" {"id" "ex:userRole"}
                                        "f:action"     [{"id" "f:view"} {"id" "f:modify"}]
                                        "f:equals"     {"@list" [{"id" "f:$identity"} {"id" "ex:User"}]}}]}]}
                                   {"id"      alice-did
                                    "ex:User" {"id" "ex:alice"}
                                    "f:role"  {"id" "ex:userRole"}}]})
                        :headers json-headers}
          txn-res      (post :transact txn-req)
          _            (assert (= 200 (:status txn-res)))
          secret-query {"select" {"?s" ["*"]}
                        "where"  [["?s" "rdf:type" "ex:User"]]}

          query-req    {:body
                        (json/write-value-as-string
                         {:ledger ledger-name
                          :query  (assoc secret-query
                                    :opts {"role" "ex:userRole"
                                           "did"  alice-did})})
                        :headers json-headers}
          query-res    (post :query query-req)]
      (is (= 200 (:status query-res))
          (str "policy-enforced query response was: " (pr-str query-res)))
      (is (= [{"id" "ex:bob", "rdf:type" ["ex:User"]}
              {"id"        "ex:alice",
               "rdf:type"  ["ex:User"],
               "ex:secret" "alice's secret"}]
             (-> query-res :body json/read-value))
          "query policy opts should prevent seeing bob's secret")
      (let [txn-req   {:body
                       (json/write-value-as-string
                        {:ledger ledger-name
                         :txn    [{"id"        "ex:alice"
                                   "ex:secret" "alice's NEW secret"}]
                         :opts   {"role" "ex:userRole"
                                  "did"  alice-did}})
                       :headers json-headers}
            txn-res   (post :transact txn-req)
            _         (assert (= 200 (:status txn-res)))
            query-req {:body
                       (json/write-value-as-string
                        {:ledger ledger-name
                         :query  secret-query})
                       :headers json-headers}
            query-res (post :query query-req)
            _         (assert (= 200 (:status query-res)))]
        (is (= [{"id"        "ex:bob",
                 "rdf:type"  ["ex:User"],
                 "ex:secret" "bob's secret"}
                {"id"        "ex:alice",
                 "rdf:type"  ["ex:User"],
                 "ex:secret" "alice's NEW secret"}]
               (-> query-res :body json/read-value))
            "alice's secret should be modified")
        (let [txn-req {:body
                       (json/write-value-as-string
                        {:ledger ledger-name
                         :txn    [{"id"        "ex:bob"
                                   "ex:secret" "bob's new secret"}]
                         :opts   {"role" "ex:userRole"
                                  "did"  alice-did}})
                       :headers json-headers}
              txn-res (post :transact txn-req)]
          (is (not= 200 (:status txn-res))
              (str "transaction policy opts should have prevented modification, instead response was:" (pr-str txn-res)))
          (let [query-req {:body
                           (json/write-value-as-string
                            {:ledger ledger-name
                             :query  {:history "ex:bob"
                                      :t       {:from 1}
                                      :opts    {"role" "ex:userRole"
                                                "did"  alice-did}}})
                           :headers json-headers}
                query-res (post :history query-req)]
            (is (= 200 (:status query-res))
                (str "History query response was: " (pr-str query-res)))
            (is (= [{"id" "ex:bob", "rdf:type" ["ex:User"]}]
                   (-> query-res :body json/read-value first (get "f:assert")))
                "policy opts should have prevented seeing bob's secret")))))))


(deftest ^:integration ^:edn policy-opts-test
  (testing "policy-enforcing opts are correctly handled"
    (let [ledger-name  (create-rand-ledger "policy-opts-test")
          edn-headers  {"Content-Type" "application/edn"
                        "Accept"       "application/edn"}
          alice-did    "did:fluree:Tf6i5oh2ssYNRpxxUM2zea1Yo7x4uRqyTeU"
          txn-req      {:body
                        (pr-str
                         {:ledger ledger-name
                          :txn    [{:id        :ex/alice,
                                    :type      :ex/User,
                                    :ex/secret "alice's secret"}
                                   {:id        :ex/bob,
                                    :type      :ex/User,
                                    :ex/secret "bob's secret"}
                                   {:id            :ex/UserPolicy,
                                    :type          [:f/Policy],
                                    :f/targetClass :ex/User
                                    :f/allow       [{:id           :ex/globalViewAllow
                                                     :f/targetRole :ex/userRole
                                                     :f/action     [:f/view]}]
                                    :f/property    [{:f/path  :ex/secret
                                                     :f/allow [{:id           :ex/secretsRule
                                                                :f/targetRole :ex/userRole
                                                                :f/action     [:f/view :f/modify]
                                                                :f/equals     {:list [:f/$identity :ex/User]}}]}]}
                                   {:id      alice-did
                                    :ex/User :ex/alice
                                    :f/role  :ex/userRole}]})
                        :headers edn-headers}
          txn-res      (post :transact txn-req)
          _            (assert (= 200 (:status txn-res)))
          secret-query '{:select {?s [:*]}
                         :where  [[?s :rdf/type :ex/User]]}

          query-req    {:body
                        (pr-str
                         {:ledger ledger-name
                          :query  (assoc secret-query
                                    :opts {:role :ex/userRole
                                           :did  alice-did})})
                        :headers edn-headers}
          query-res    (post :query query-req)]
      (is (= 200 (:status query-res))
          (str "policy-enforced query response was: " (pr-str query-res)))
      (is (= [{:id       :ex/bob
               :rdf/type [:ex/User]}
              {:id        :ex/alice
               :rdf/type  [:ex/User]
               :ex/secret "alice's secret"}]
             (-> query-res :body edn/read-string))
          "query policy opts should prevent seeing bob's secret")
      (let [txn-req   {:body
                       (pr-str
                        {:ledger ledger-name
                         :txn    [{:id        :ex/alice
                                   :ex/secret "alice's NEW secret"}]
                         :opts   {:role :ex/userRole
                                  :did  alice-did}})
                       :headers edn-headers}
            txn-res   (post :transact txn-req)
            _         (assert (= 200 (:status txn-res)))
            query-req {:body
                       (pr-str
                        {:ledger ledger-name
                         :query  secret-query})
                       :headers edn-headers}
            query-res (post :query query-req)
            _         (assert (= 200 (:status query-res)))]
        (is (= [{:id        :ex/bob
                 :rdf/type  [:ex/User]
                 :ex/secret "bob's secret"}
                {:id        :ex/alice
                 :rdf/type  [:ex/User]
                 :ex/secret "alice's NEW secret"}]
               (-> query-res :body edn/read-string))
            "alice's secret should be modified")
        (let [txn-req {:body
                       (pr-str
                        {:ledger ledger-name
                         :txn    [{:id        :ex/bob
                                   :ex/secret "bob's NEW secret"}]
                         :opts   {:role :ex/userRole
                                  :did  alice-did}})
                       :headers edn-headers}
              txn-res (post :transact txn-req)]
          (is (not= 200 (:status txn-res))
              (str "transaction policy opts should have prevented modification, instead response was:" (pr-str txn-res)))
          (let [query-req {:body
                           (pr-str
                            {:ledger ledger-name
                             :query  {:history :ex/bob
                                      :t       {:from 1}
                                      :opts    {:role :ex/userRole
                                                :did  alice-did}}})
                           :headers edn-headers}
                query-res (post :history query-req)]
            (is (= 200 (:status query-res))
                (str "History query response was: " (pr-str query-res)))
            (is (= [{:id :ex/bob :rdf/type [:ex/User]}]
                   (-> query-res :body edn/read-string first (get :f/assert)))
                "policy opts should have prevented seeing bob's secret")))))))
