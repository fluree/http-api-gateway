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
                              :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                              :f      "https://ns.flur.ee/ledger#"}}}}}))

(defn run-test-server
  [run-tests]
  (swap! api-port find-open-port)
  (let [stop-server (sys/run-server {:profile :test})]
    (run-tests)
    (stop-server)))

(use-fixtures :once run-test-server)

(def commit-id-regex
  (re-pattern "fluree:commit:sha256:[a-z2-7]{52,53}"))

(def mem-addr-regex
  (re-pattern "fluree:memory://[a-f0-9]{64}"))

(def context-id-regex
  (re-pattern "fluree:context:[a-f0-9]{64}"))

(def db-id-regex
  (re-pattern "fluree:db:sha256:[a-z2-7]{52,53}"))

(defn api-url [endpoint]
  (str "http://localhost:" @api-port "/fluree/" (name endpoint)))

(defn post [endpoint req]
  (http/post (api-url endpoint) (assoc req :throw-exceptions false)))

(defn create-rand-ledger
  [name-root]
  (let [ledger-name (str name-root "-" (random-uuid))
        req         (pr-str {:ledger         ledger-name
                             :defaultContext ["" {:foo "http://foobar.com/"}]
                             :txn            [{:id       :foo/create-test
                                               :type     :foo/test
                                               :foo/name "create-endpoint-test"}]})
        headers     {"Content-Type" "application/edn"
                     "Accept"       "application/edn"}
        res         (update (post :create {:body req :headers headers})
                            :body edn/read-string)]
    (if (= 201 (:status res))
      (get-in res [:body :alias])
      (throw (ex-info "Error creating random ledger" res)))))

(deftest ^:integration ^:json create-endpoint-json-test
  (testing "can create a new ledger w/ JSON"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (json/write-value-as-string
                       {"ledger"         ledger-name
                        "defaultContext" ["" {"foo" "http://foobar.com/"}]
                        "txn"            [{"id"      "ex:create-test"
                                           "type"    "foo:test"
                                           "ex:name" "create-endpoint-test"}]})
          headers     {"Content-Type" "application/json"
                       "Accept"       "application/json"}
          res         (post :create {:body req :headers headers})]
      (is (= 201 (:status res)))
      (is (= {"address" address
              "alias"   ledger-name
              "t"       1}
             (-> res :body json/read-value))))))

(deftest ^:integration ^:edn create-endpoint-edn-test
  (testing "can create a new ledger w/ EDN"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (pr-str {:ledger         ledger-name
                               :defaultContext ["" {:foo "http://foobar.com/"}]
                               :txn            [{:id      :ex/create-test
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
          req         (pr-str {:ledger         ledger-name
                               :defaultContext ["" {:foo "http://foobar.com/"}]
                               :txn            [{:id      :ex/create-test
                                                 :type    :foo/test
                                                 :ex/name "create-endpoint-test"}]})
          headers     {"Content-Type" "application/edn"
                       "Accept"       "application/edn"}
          res-success (post :create {:body req :headers headers})
          _           (assert (= 201 (:status res-success)))
          res-fail    (post :create {:body req :headers headers})]
      (is (= 409 (:status res-fail))))))

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

(deftest ^:integration ^:json history-query-json-test
  (testing "basic JSON history query works"
    (let [ledger-name   "history-query-json-test"
          json-headers  {"Content-Type" "application/json"
                         "Accept"       "application/json"}
          txn-req       {:body
                         (json/write-value-as-string
                          {"ledger" ledger-name
                           "txn"    [{"id"      "ex:query-test"
                                      "type"    "schema:Test"
                                      "ex:name" "query-test"}]})
                         :headers json-headers}
          txn-res       (post :create txn-req)
          _             (assert (= 201 (:status txn-res)))
          txn2-req      {:body
                         (json/write-value-as-string
                          {"ledger" ledger-name
                           "txn"    [{"id"           "ex:query-test"
                                      "ex:test-type" "integration"}]})
                         :headers json-headers}
          txn2-res      (post :transact txn2-req)
          _             (assert (= 200 (:status txn2-res)))
          query-req     {:body
                         (json/write-value-as-string
                          {"ledger" ledger-name
                           "query"  {"commit-details" true
                                     "t"              {"at" "latest"}}})
                         :headers json-headers}
          query-res     (post :history query-req)
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
          edn-headers   {"Content-Type" "application/edn"
                         "Accept"       "application/edn"}
          txn-req       {:body
                         (pr-str
                          {:ledger ledger-name
                           :txn    [{:id      :ex/query-test
                                     :type    :schema/Test
                                     :ex/name "query-test"}]})
                         :headers edn-headers}
          txn-res       (post :create txn-req)
          _             (assert (= 201 (:status txn-res)))
          txn2-req      {:body
                         (pr-str
                          {:ledger ledger-name
                           :txn    [{:id           :ex/query-test
                                     :ex/test-type "integration"}]})
                         :headers edn-headers}
          txn2-res      (post :transact txn2-req)
          _             (assert (= 200 (:status txn2-res)))
          query-req     {:body
                         (pr-str
                          {:ledger ledger-name
                           :query  {:commit-details true
                                    :t              {:from 1}}})
                         :headers edn-headers}
          query-res     (post :history query-req)
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

(deftest ^:integration ^:json policy-opts-json-test
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
                        {"ledger" ledger-name
                         "txn"    [{"id"        "ex:bob"
                                    "ex:secret" "bob's new secret"}]
                         "opts"   {"role" "ex:userRole"
                                   "did"  alice-did}})
                       :headers json-headers}
              txn-res (post :transact txn-req)]
          (is (not= 200 (:status txn-res))
              (str "transaction policy opts should have prevented modification, instead response was: " (pr-str txn-res)))
          (let [query-req {:body
                           (json/write-value-as-string
                            {"ledger" ledger-name
                             "query"  {"history" "ex:bob"
                                       "t"       {"from" 1}
                                       "opts"    {"role" "ex:userRole"
                                                  "did"  alice-did}}})
                           :headers json-headers}
                query-res (post :history query-req)]
            (is (= 200 (:status query-res))
                (str "History query response was: " (pr-str query-res)))
            (is (= [{"id" "ex:bob", "rdf:type" ["ex:User"]}]
                   (-> query-res :body json/read-value first (get "f:assert")))
                "policy opts should have prevented seeing bob's secret")))))))

(deftest ^:integration ^:edn policy-opts-edn-test
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
