(ns merkats.clients.bybit.public-data
  "Small utility namespace to download public data from bybit"
  (:require [hato.client :as http]
            [clojure.java.io :as io])
  (:import (java.util.zip GZIPInputStream)))

(def ^:private url "https://public.bybit.com")

(def ^:private index->path {:trading "trading"
                  :spot "spot_index"
                  :premium "premium_index"})

(def ^:private index->symbol-suffix {:trading ""
                                     :spot "_index_price"
                                     :premium "_premium_index"})

(defn- file-url
  "Given `index` (index to retrieve data from), `instr` as the instrument symbol and `date`, 
   returns the exact url of the file with public data."
  [index instr date-str]
  (format "%s/%s/%s/%s%s%s.csv.gz"
          url
          (index->path index)
          instr
          instr
          date-str
          (index->symbol-suffix index)))

(defn download!
  "Return input stream of csv.gzip file, given opts map:
   - `:index` e/o #{:trading :spot :premium}
   - `:market` instrument symbol to download data from
   - `:date-str` date to download as string in format 'YYYY-MM-DD'

   See: https://public.bybit.com/

   Note: The input stream is from a gzipped file, you have to pipe it through a GZipInputStream
   to read it line by line.

   Note: Some files might come in reverse order, first line (after csv header) being last event of 
   the day. (Observed in :trading index 'BTCUSD' market, where format was changed in 07-12-2021 from
   decreasing time to increasing time, also changing timestamp from seconds.micros to only seconds).

   Example:

   ```clojure
   (->> (download! :trading \"BTCUSD\" \"2020-01-01\")
        (java.util.zip.GZIPInputStream.) 
        (clojure.java.io/reader)
        (line-seq))
   ```"
  [{:keys [market index date-str]}]
  (let [file-url (file-url index market date-str)
        resp (http/get file-url {:as :stream})]
    (:body resp)))

(defn download-as-lines!
  "Downloads the content of a file using [[download!]] and return its lines as seq, dropping the 
   headers line.
   
   opts map: See [[download!]]"
  [{:keys [market index date-str] :as opts}]
  (->> (download! opts)
       (GZIPInputStream.)
       (io/reader)
       (line-seq)
       (drop 1)))

(comment
  (file-url :spot "BTCUSD" "2020-01-01")
  (file-url :trading "BTCUSD" "2020-01-01")
  (file-url :premium "BTCUSD" "2020-01-01")

  (take 10 (download-as-lines! {:index :trading
                                :market "BTCUSD"
                                :date-str "2022-02-06"})))
