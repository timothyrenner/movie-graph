(import click)
(import json)
(import re)

(import [toolz [get :as tget get-in]])
(import [loguru [logger]])
(import [dateutil.parser [parse :as parse-date]])
(import [pytimeparse [parse :as parse-time]])


(defn convert-date [date-str]
    (-> date-str parse-date (.strftime "%Y-%m-%d")))


(defn convert-time [time-str]
    (-> time-str (parse-time) (/ 60) int))


(defn split-commas [comma-sep-str]
    (as-> comma-sep-str s
        (.split s ",")
        (map (fn [x] (x.strip)) s)
        (list s)))


(defn match-writer-role [writer-role-str]
    (cond
        [(in "screenplay" writer-role-str) "screenplay"]
        [(in "story" writer-role-str) "story"]
        [(in "characters" writer-role-str) "characters"]
        [(in "play" writer-role-str) "play"]
        [(in "material" writer-role-str) "material"]
        [(in "dialogue" writer-role-str) "dialogue"]
        [True None]))


(defn get-writer [writer-str]
    (if
        (setx writer-match 
            (re.match r"(.*) \((.*)\)" writer-str))
        {
            "name" (.strip (writer-match.group 1))
            "role" (-> writer-match (.group 2) (.strip) (match-writer-role))
        }
        {"name" (.strip writer-str)}))


(defn merge-airtable-omdb [airtable-record omdb-record]
    {
        "title" (tget "Title" omdb-record)
        "year" (int (tget "Year" omdb-record))
        "release-date" (convert-date (tget "Released" omdb-record))
        "runtime-minutes" (convert-time (tget "Runtime" omdb-record))
        "genre" (split-commas (tget "Genre" omdb-record))
        "country" (tget "Country" omdb-record)
        "director" (split-commas (tget "Director" omdb-record))
        "actors" (split-commas (tget "Actors" omdb-record))
        "language" (split-commas (tget "Language" omdb-record))
        "production" (tget "Production" omdb-record)
        "writer" (->> 
                    (tget "Writer" omdb-record) 
                    split-commas 
                    (map get-writer) 
                    list)
        "tags" (get-in ["fields" "Tags"] airtable-record)
        "watched" (get-in ["fields" "Watched"] airtable-record)
        "ratings" (+
            (lfor rating (tget "Ratings" omdb-record) 
                {
                    "source" (tget "Source" rating) 
                    "value" (tget "Value" rating)
                })
            [{
                "source" "me" 
                "value" (get-in ["fields" "Rating"] airtable-record)
            }])
        "service" (get-in ["fields" "Service" 0] airtable-record)
        "imdb-link" (get-in ["fields" "IMDB Link"] airtable-record)
    })


#@(
    (click.command)
    (click.option
        "--airtable-file"
        :type (click.File "r")
        :default "data/raw/airtable_out.json")
    (click.option
        "--omdb-file"
        :type (click.File "r")
        :default "data/raw/omdb_out.json")
    (click.option
        "--output-file"
        :type (click.File "w")
        :default "data/interim/merged_records.json")
    (defn main [airtable-file omdb-file output-file]
        ;; Build a big dict for the airtable records.
        (setv
            airtable-records
                (dfor record (map json.loads airtable-file)
                    [(tget "id" record) record])
            omdb-records
                (dfor record (map json.loads omdb-file)
                    [(tget "airtable-id" record) record])
            ;; All the IDs in a sequence so we can loop over them.
            all-ids (airtable-records.keys))
        (logger.info f"Number of records: {(len airtable-records)}.")
        (for [id all-ids]
            (setv 
                omdb-record (tget id omdb-records)
                airtable-record (tget id airtable-records)
                full-record (merge-airtable-omdb airtable-record omdb-record))
            (output-file.write (json.dumps full-record))
            (output-file.write "\n"))
        (logger.info "All done!")))


(if (= __name__ "__main__") (main))