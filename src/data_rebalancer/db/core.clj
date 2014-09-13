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

;シャード構成済みか確認
(defn get-group [groupname]
  (first (select groups
           (where {:name groupname})
           (limit 1))))

;ノードのハッシュ値を取得する
(defn- get-hash [dialect url user password hashfunction seed]
  (let [database {:classname "oracle.jdbc.OracleDriver"
                  :subprotocol dialect
                  :subname url
                  :user user
                  :password password}]
    (->> (str "SELECT " (format hashfunction (str "'" seed "'")) " AS hashvalue " "FROM DUAL")
      (sql/query database) (first) (:hashvalue))))

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
                (values [ {:shardid shardid :virtualname (str url username x)
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
(defn search-shards [groupname]
  (letfn
    [(merge-count [shard]
      (let [ group (get-group groupname)
           database {:classname "oracle.jdbc.OracleDriver"
                            :subprotocol (:database group)
                            :subname (:url shard)
                            :user (:user shard)
                            :password (:password shard)}
           query (format "select count(*) as cnt , max(%s) as mx, min(%s) as mn from %s"
                   (format (:hashfunction group) (:keycolumn group)) (format (:hashfunction group) (:keycolumn group)) groupname)]
         (->>  (sql/query database query) (first) (merge shard))))
     (merge-virtual [shard]
       (merge shard {:virtuals (select virtualshards (where {:shardid (:id shard)}))}))]
    (->> (select shards (where {:groupname groupname}) (order :hashvalue :DESC) ) (map merge-count) (map merge-virtual))))

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

;sourceからdestにレコードを移動する
(defn- move-record [group direction destdb sourcedb hash]
     (let [groupname (:name group)
           hashcolumn (format (:hashfunction group) (:keycolumn group))
           selectquery (format "SELECT * FROM %s WHERE %s %s ? " groupname hashcolumn direction)]
       (with-open [sourcecon (sql/get-connection sourcedb)
                   sourcestmt (sql/prepare-statement sourcecon selectquery)]
         (.setFetchSize sourcestmt 1000)
         (.setObject sourcestmt 1 hash)
         (println (:subname sourcedb) "/" (:user sourcedb) " = " selectquery ":" hash "->" (:subname destdb) "/" (:user destdb))
         ;抽出もとから抜いて自分に一括登録
         (with-open [rseq (.executeQuery sourcestmt)]
           (transaction
             ;ここは現実化しないと大変なことになる
             (doall (for [splited (partition-all 10000 (resultset-seq rseq))]
                (apply sql/db-do-prepared destdb
                    (format "INSERT INTO %s VALUES (%s)" groupname
                      (clojure.string/join ", " (repeat (.getColumnCount (.getMetaData rseq)) "?" )))
                    (for [row splited] (vals row)))))))
         ;元データの消し込み
         (sql/execute! sourcedb
           [(format "DELETE FROM %s WHERE %s %s ? " groupname hashcolumn direction) hash]))))

; 最大hash値を管理するシャードから順に各シャード内の自分以上のhash値を持つレコードをINSERT AND DELETE
; 最小hash値を管理するシャードが、自分の次の大きさのシャード内の自分のhashより小さいレコードをINSERT AND DELETE
(defn rebalance [groupname]
  (let [group (first (select groups (where {:name groupname})))
        shards (select shards (where {:groupname groupname}) (order :hashvalue :DESC ))]
    (letfn [(move [direction dest source-shards hashfunction]
          (when-not (empty? source-shards)
            (let [destdb {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group)
                       :subname (:url dest) :user (:user dest) :password (:password dest)}]
              (doall (for [source source-shards]
                  (move-record group direction destdb
                    {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group) :subname (:url source)
                     :user (:user source) :password (:password source)} (hashfunction dest source))))
               (recur direction (first source-shards) (rest source-shards) hashfunction))))]
          ;降順に実行
          (move ">=" (first shards) (rest shards) (fn [dest source] (:hashvalue dest))  )
          ;昇順に実行
          (move "<" (first (reverse shards)) (rest (reverse shards))  (fn [dest source] (:hashvalue source))  ))))

;削除処理
(defn release [groupname url user]
  (transaction
    (let [group (first (select groups (where {:name groupname})))
          groupshards (select shards (where {:groupname groupname}))
          current (first (select shards (where {:groupname groupname :url url :user user})))
          sourcedb {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group) :subname (:url current)
                    :user (:user current) :password (:password current)}
          nearlestLess (first (select shards (where (and (= :groupname groupname) (< :hashvalue (:hashvalue current)))) (order :hashvalue :DESC) (limit 1) ))]
      (if (or (empty? group) (>= 1 (count groupshards)))
        (throw (IllegalStateException. "can' remove because this is last one"))
        (if (empty? nearlestLess)
          ;一つ前のものがなければ、1個上のものに全件移動     TODO
          (let [nearlestBigger (first (select shards (where (and (= :groupname groupname) (> :hashvalue (:hashvalue current)))) (order :hashvalue :ASC) (limit 1) ))
                biggerdb {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group) :subname (:url nearlestBigger)
                            :user (:user nearlestBigger) :password (:password nearlestBigger)}]
              (move-record group ">=" biggerdb sourcedb 0))
          ;現在のノードのレコードを一つhashが小さい前のノードに全件移動する
          (let [leastdb {:classname "oracle.jdbc.OracleDriver" :subprotocol (:database group) :subname (:url nearlestLess)
                           :user (:user nearlestLess) :password (:password nearlestLess)}]
              (move-record group ">=" leastdb sourcedb 0))))
     (delete shards (where {:id (:id current)})))))


