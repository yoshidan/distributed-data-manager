(ns data-rebalancer.db.core
  (:import [java.sql Connection DriverManager PreparedStatement Statement])
  (:use korma.core
        [korma.db :only (defdb transaction)])
  (:require [data-rebalancer.db.schema :as schema]
            [clojure.java.jdbc :as sql]))

(defdb db schema/db-spec)
(defentity groups)
(defentity shards)

;シャード構成済みか確認
(defn get-group [groupname]
  (first (select groups
           (where {:name groupname})
           (limit 1))))

;ノードのハッシュ値を取得する
(defn get-hash [dialect url user password nodenumber hashfunction]
  (let [database {:classname "oracle.jdbc.OracleDriver"
                  :subprotocol dialect
                  :subname url
                  :user user
                  :password password}]
    (->> (str "SELECT " (format hashfunction nodenumber) " AS hashvalue " "FROM DUAL")
      (sql/query database) (first) (:hashvalue))))

; グループを初期化する
(defn init-group [no groupname url user password hashfunction database virtualcount keycolumn]
  (println no groupname url user password hashfunction database virtualcount keycolumn)
  (transaction
    (insert groups
      (values [
              {:name groupname :hashfunction hashfunction :database database :virtualcount virtualcount :keycolumn keycolumn}]))
    (insert shards
      (values [ {:groupname groupname :url url :user user :password password :hashvalue (get-hash database url user password no hashfunction) } ]))))

;構成済みのシャード一覧を取得する
(defn search-shards [groupname]
  (letfn [(get-count [shard]
    (let [database {:classname "oracle.jdbc.OracleDriver"
                            :subprotocol (:database (get-group groupname))
                            :subname (:url shard)
                            :user (:user shard)
                            :password (:password shard)}]
      (->> (str "select count(*) as cnt from " groupname) (sql/query database) (first) (:cnt) (hash-map :count) (merge shard))))]
    (->> (select shards (where {:groupname groupname})) (map get-count))))

;構成にシャードを追加する
(defn add-shard [groupname url username password]
  (if (empty? (select shards (where {:groupname groupname :url url :user username})))
    (insert shards (values [{:groupname groupname :url url :user username :password password
                           :hashvalue (get-hash (:database (get-group groupname)) url username password 1 (:hashfunction (get-group groupname)))}]))
    (throw (Exception. "already exists"))) )

;ddlを表示する
(defn show-ddl [groupname url user]
  (let [^Connection connection (DriverManager/getConnection url user (:password (first (select shards
                                                                                   (where {:groupname groupname :url url :user user})))))
        ^PreparedStatement stmt (.prepareStatement connection "SELECT to_char(dbms_metadata.get_ddl('TABLE', ? )) AS DDL FROM DUAL")]
        (.setString stmt 1 groupname)
        (->> (.executeQuery stmt) (resultset-seq) (map :ddl) )))


  ;指定されたデータソースの検索処理、とりあえずOracleのみ対応
(defn search [url username password dialect]
  (let [database {:classname "oracle.jdbc.OracleDriver"
                  :subprotocol dialect
                  :subname url
                  :user username
                  :password password}]
      (letfn [(get-columns [tablename]
                (->> (sql/query database ["SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?" tablename] )
                    ;現実化しないと先頭1件のカラムしか拾えない
                     (map :column_name) (apply list)))
              (get-shards [groupname]
                (count (select shards (where {:groupname groupname}))))]
        (with-open [con (sql/get-connection database)
                    stmt (sql/prepare-statement con "SELECT TABLE_NAME FROM USER_TABLES")]
            (.setFetchSize stmt 100)
            (with-open [rset (.executeQuery stmt)]
              ;doallで現実化して全件取得しないとクローズされる
              (doall (for [t (->> (resultset-seq rset) (map :table_name))]
                (let [g (get-group t)]
                  (if (empty? g)
                      (array-map :group g :table t :columns (get-columns t))
                      (array-map :group g :table t :shards (get-shards t)))))))))))
