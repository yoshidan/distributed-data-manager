(ns data-rebalancer.db.core
  (:import [java.sql Connection DriverManager PreparedStatement Statement])
  (:use korma.core
        [korma.db :only (defdb transaction)])
  (:require [data-rebalancer.db.schema :as schema]
            [clojure.java.jdbc :as sql]))

(defdb db schema/db-spec)
(defentity groups)
(defentity shards)
(defentity virtualshards)

;ノードのハッシュ値を取得する
(defn- get-hash [dialect url user password hashfunction seed]
  (let [database {:classname "oracle.jdbc.OracleDriver"
                  :subprotocol dialect
                  :subname url
                  :user user
                  :password password}]
    (->> (str "SELECT " (format hashfunction (str "'" seed "'")) " AS hashvalue " "FROM DUAL")
      (sql/query database) (first) (:hashvalue))))


;シャード構成済みか確認
(defn get-group [groupname]
  (first (select groups
           (where {:name groupname})
           (limit 1))))

;構成にシャードを追加する
(defn add-shard [groupname url username password]
  (if (empty? (select shards (where {:groupname groupname :url url :user username})))
    (do
      (let [group (get-group groupname)]
        (transaction
          (insert shards (values [{:groupname groupname :url url :user username :password password
                                   :hashvalue (get-hash (:database group) url username password
                                                (:hashfunction group) (str url username))}]))
          ;virtual shards
          (let [shardid (:id (first (select shards (where {:groupname groupname :url url :user username}))))]
            (doall (for [x (range (:virtualcount group))]
              (insert virtualshards
                (values [ {:shardid shardid :virtualname (str username x)
                           :hashvalue (get-hash (:database group) url username password
                                        (:hashfunction group) (str url username x))}]))))))))
    (throw (IllegalStateException. "already exists"))) )

; グループを初期化する
(defn init-group [groupname url user password hashfunction database virtualcount keycolumn]
  (transaction
    (insert groups
      (values [
              {:name groupname :hashfunction hashfunction :database database :virtualcount virtualcount :keycolumn keycolumn}]))
    (add-shard groupname url user password)))

;構成済みのシャード一覧を取得する
(defn search-physical-shards [groupname]
  (letfn
    [(merge-count [shard]
      (let [ group (get-group groupname)
             database {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group) :subname (:url shard) :user (:user shard) :password (:password shard)}
             query (format "select count(*) as cnt from %s" groupname)]
         (->>  (sql/query database query) (first) (merge shard))))]
    (->> (select shards (where {:groupname groupname}) (order :hashvalue :DESC) ) (map merge-count))))

(defn search-shards [groupname]
  (let [ shards-asc (asc-shards groupname)
         betweens (map #(list %1 %2) shards-asc (conj (into [] (rest shards-asc)) (first shards-asc)))]
    (letfn [(merge-count [betweens]
        (let [shard (first betweens)
              dest (second betweens)
              group (get-group groupname)
              hc (format (:hashfunction group) (:keycolumn group))
              database {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group) :subname (:url shard) :user (:user shard) :password (:password shard)}
              query (if (> (Integer/parseInt (:hashvalue shard)) (Integer/parseInt (:hashvalue dest)))
                      (format "select count(*) as cnt , max(%s) as mx, min(%s) as mn from %s where %s >= ? or %s < ?" hc hc groupname hc hc)
                      (format "select count(*) as cnt , max(%s) as mx, min(%s) as mn from %s where %s between ? and ?" hc hc groupname hc))]
            (->> (sql/query database [query (:hashvalue shard) (:hashvalue dest)]) (first) (merge shard) (merge shard {:physical (empty? (:groupanme shard)) :limithashvalue (:hashvalue dest)}) )))]
      (map merge-count betweens))))

;指定されたデータソースの検索処理、とりあえずOracleのみ対応
(defn search [url username password dialect]
  (let [database {:classname "oracle.jdbc.OracleDriver" :subprotocol dialect :subname url :user username :password password}]
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

;物理シャードと仮想シャードを併せて昇順にする
(defn- asc-shards [groupname]
  (sort-by :hashvalue
    (apply concat
      (for [shard (select shards (where {:groupname groupname}))]
          (cons shard
            (for [v (select virtualshards (where {:shardid (:id shard)}))]
                {:hashvalue (:hashvalue v) :url (:url shard) :user (:user shard ) :password (:password shard)}))))))

;レコード移動
(defn- move [group source dest sourcedb destdb r]
  (let [groupname (:name group)
        hashcolumn (format (:hashfunction group) (:keycolumn group))
        selectquery (if (> (Integer/parseInt (:hashvalue (first r))) (Integer/parseInt (:hashvalue (second r))))
                        (format "SELECT * FROM %s WHERE %s >= ? OR %s < ? " groupname hashcolumn hashcolumn)
                        (format "SELECT * FROM %s WHERE %s BETWEEN ? AND ? " groupname hashcolumn))
        deletequery (if (> (Integer/parseInt (:hashvalue (first r))) (Integer/parseInt (:hashvalue (second r))))
                       (format "DELETE FROM %s WHERE %s >= ? OR %s < ? " groupname hashcolumn hashcolumn)
                       (format "DELETE FROM %s WHERE %s BETWEEN ? AND ? " groupname hashcolumn))]
      (with-open [sourcecon (sql/get-connection sourcedb)
                  sourcestmt (sql/prepare-statement sourcecon selectquery)]
        (.setFetchSize sourcestmt 1000)
        (.setObject sourcestmt 1 (:hashvalue (first r)))
        (.setObject sourcestmt 2 (:hashvalue (second r)))
        (println (:subname sourcedb) "/" (:user sourcedb) "/" (:hashvalue source)  " = " selectquery (:hashvalue (first r)) (:hashvalue (second r)) "->" (:subname destdb) "/" (:user destdb) "/" (:hashvalue dest) )
        ;抽出もとから抜いて自分に一括登録
        (with-open [rseq (.executeQuery sourcestmt)]
          (transaction
            ;ここは現実化しないと先頭1回しか実行しない
            (doall (for [splited (partition-all 10000 (resultset-seq rseq))]
                     (apply sql/db-do-prepared destdb
                       (format "INSERT INTO %s VALUES (%s)" groupname
                         (clojure.string/join ", " (repeat (.getColumnCount (.getMetaData rseq)) "?" )))
                       (for [row splited] (vals row)))))))
        ;元データの消し込み(なぜか削除件数はPersistentListが返却されるためfirstで先頭取得する）
        (first (sql/execute! sourcedb [deletequery (:hashvalue (first r)) (:hashvalue (second r))])))))

;リバランス処理
(defn rebalance [groupname]
  (let [group (first (select groups (where {:name groupname})))
        shardsAsc (asc-shards groupname)
        betweens (map #(list %1 %2) shardsAsc (conj (into [] (rest shardsAsc)) (first shardsAsc)))]
    (apply + (for [r betweens]
      (let [dest (first r)]
        ;投入先以外のノードから管理対象の値を引っこ抜く
        (apply + (for [source (filter #(and (not (empty? (:groupname %))) (not (= % dest))) shardsAsc) ]
          (let [
            sourcedb {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group) :subname (:url source) :user (:user source) :password (:password source)}
            destdb {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group) :subname (:url dest) :user (:user dest) :password (:password dest)}]
            (if (= destdb sourcedb)
              0
              (move group source dest sourcedb destdb r))))))))))

;削除処理
;他の全てのノードから削除対象のノード野範囲を取得しにくる
(defn release [groupname url user]
  (transaction
    (let [group (first (select groups (where {:name groupname})))
          groupshards (select shards (where {:groupname groupname}))
          current (first (select shards (where {:groupname groupname :url url :user user})))
          sourcedb {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group) :subname (:url current) :user (:user current) :password (:password current)}]

      (if (or (empty? group) (>= 1 (count groupshards)))
        (throw (IllegalStateException. "can' remove because this is last one"))
        (do
          ;まず不要シャード削除
          (delete virtualshards (where {:shardid (:id current)}))
          (delete shards (where {:id (:id current)}))

          ;削除したデータの入れ先達
          (let [shardsAsc (asc-shards groupname)
                betweens (map #(list %1 %2) shardsAsc (conj (into [] (rest shardsAsc)) (first shardsAsc)))]
            (apply + (for [r betweens]
               (let [dest (first r)
                     destdb {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group) :subname (:url dest) :user (:user dest) :password (:password dest)}]
                 (move group current dest sourcedb destdb r))))))))))

