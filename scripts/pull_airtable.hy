(import os)
(import requests)
(import click)
(import json)
(import [dotenv [find-dotenv load-dotenv]])

(require [hy.contrib.loop [loop]])

;; Load the dotenv file.
(load-dotenv (find-dotenv))

(setv base-id "appO4GwbQofzz0lnO")
(setv api-key (os.getenv "AIRTABLE_KEY"))
(setv airtable-endpoint "https://api.airtable.com/v0")



(defn airtable-get [url session &optional offset]
    (if offset
        (session.get url :params {"view" "Have Watched" "offset" offset})
        (session.get url :params {"view" "Have Watched"})))

(defn airtable-get-all [url session]
    (setv records [])
    (loop [[offset None]]
        ;; First grab the response from airtable.
        (setv airtable-json 
            (-> url
            (airtable-get session :offset offset)
            (.json)))
        ;; Extend our list of all records.
        (records.extend (airtable-json.get "records" []))
        ;; If there's an offset field in the response, we need to paginate.
        ;; Rinse and repeate with the next offset.
        ;; If there's not an offset field in the response, we can return the
        ;; records list.
        (if (setx next-offset (airtable-json.get "offset" None))
            (recur next-offset)
            records)))

#@(
    (click.command)
    (click.option 
        "--output-file" "-o" 
        :type (click.File "w")
        :default "data/raw/airtable_out.json")
    (defn main [output-file]
        ;; Create the requests session object.
        (setv 
            session (requests.Session)
            request-url f"{airtable-endpoint}/{base-id}/Movies")
        (session.headers.update {"Authorization" f"Bearer {api-key}"})
        (setv airtable-records (airtable-get-all request-url session))
        (for [record airtable-records]
            (output-file.write (json.dumps record))
            (output-file.write "\n"))))


(if (= __name__ "__main__") (main))