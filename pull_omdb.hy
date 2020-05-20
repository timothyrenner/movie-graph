(import click)
(import os)
(import requests)
(import json)

(import [dotenv [find-dotenv load-dotenv]])
;; We can't override the get builtin for hy.
(import [toolz [pluck get :as tget assoc :as tassoc get-in]])
(import [requests.exceptions [RequestException]])
(import [time [sleep]])

;; Load the dotenv file and get the api key for OMDB.
(load-dotenv (find-dotenv))
(setv 
    api-key (os.getenv "OMDB_KEY")
    omdb-endpoint "http://www.omdbapi.com")

(defn load-cache [file]
    (with [f (open file "r")]
        (f.readlines)))

(defn load-entries [file]
    (with [f (open file "r")]
        (->> f (map json.loads) (list))))

(defn get-imdb-id [imdb-url]
    (as-> imdb-url i (i.split "/") (tget -2 i)))

(defn get-omdb [movie-id session]
    (-> session
        (.get omdb-endpoint :params {"apikey" api-key "i" movie-id})
        (.json)))

#@(
    (click.command)
    (click.option
        "--airtable-file" "-a"
        :type (click.File "r")
        :default "data/raw/airtable_out.json")
    (click.option
        "--output-file" "-o"
        :type str
        :default "data/raw/omdb_out.json")
    (click.option
        "--skip-cache"
        :is-flag True
        :default False)
    (defn main [airtable-file output-file skip-cache]
        ;; Load the "cache" if needed.
        (setv 
            imdb-entries (if skip-cache [] (load-entries output-file))
            ids (->> imdb-entries (pluck "imdb-id") set))
        ;; Create a requests Session object for repeated calls to omdb api.
        (setv session (requests.Session))
        ;; Loop over the airtable results and add them to the list of imdb
        ;; data entries if they aren't already present.
        (for [airtable-entry (map json.loads airtable-file)]
            (setv
                airtable-id (tget "id" airtable-entry)
                imdb-id (get-imdb-id 
                    (get-in ["fields" "IMDB Link"] airtable-entry)))
            (unless (in imdb-id ids)
                (print f"Pulling OMDB data for {imdb-id}.")
                (try
                    (setv imdb-entry
                        ;; Get the OMDB information and associate the airtable
                        ;; ID with it so we can skip that title next time.
                        (-> imdb-id 
                            (get-omdb session) 
                            (tassoc "airtable-id" airtable-id)))
                    (.append imdb-entries imdb-entry)
                    (sleep 0.1)
                    ;; Skip the append if we couldn't get any data.
                    (except [RequestException]
                        (print f"Request for {imdb-id} failed.")
                        (continue)))))
        ;; Write the omdb responses to a file. This will include everything.
        ;; Eventually we might want to just append these to like a database
        ;; or something but this'll do for now.
        (with [f (open output-file "w")]
            (for [imdb-entry imdb-entries]
                (-> imdb-entry json.dumps f.write)
                (f.write "\n")))))

(if (= __name__ "__main__") (main))