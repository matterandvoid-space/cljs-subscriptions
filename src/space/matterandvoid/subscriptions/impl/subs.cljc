(ns space.matterandvoid.subscriptions.impl.subs
  (:require
    [space.matterandvoid.subscriptions.impl.interop :refer [add-on-dispose! debug-enabled? make-reaction ratom? deref? dispose! reagent-id
                                                            reactive-context?]]
    [space.matterandvoid.subscriptions.impl.loggers :refer [console]]
    [space.matterandvoid.subscriptions.impl.trace :as trace :include-macros true]))

;; -- cache -------------------------------------------------------------------

(defn clear-subscription-cache!
  "calls `on-dispose` for each cached item,
   which will cause the value to be removed from the cache"
  [get-subscription-cache app]
  (console :info "Clearing subscription cache")
  (doseq [[_ rxn] @(get-subscription-cache app)] (dispose! rxn))
  (if (not-empty @(get-subscription-cache app))
    (console :warn "re-frame: The subscription cache isn't empty after being cleared")))

;; De-duplicate subscriptions. If two or more equal subscriptions
;; are concurrently active, we want only one handler running.
;; Two subscriptions are "equal" if their query vectors test "=".

(defn cache-and-return!
  "cache the reaction r"
  [get-subscription-cache app query-v reaction]
  ;; this prevents memory leaks (caching subscription -> reaction) but still allows
  ;; executing outside of a (reagent.reaction ) form, like in event handlers.
  (when (reactive-context?)
    ;(console :debug "IN A REACTIVE CONTEXT")
    (let [cache-key          query-v
          subscription-cache (get-subscription-cache app)]
      ;(console :debug "cache-and-return!" #_subscription-cache)
      ;; when this reaction is no longer being used, remove it from the cache
      (add-on-dispose! reaction #(trace/with-trace {:operation (first query-v)
                                                    :op-type   :sub/dispose
                                                    :tags      {:query-v  query-v
                                                                :reaction (reagent-id reaction)}}
                                   (swap! subscription-cache
                                     (fn [query-cache]
                                       (if (and (contains? query-cache cache-key) (identical? reaction (get query-cache cache-key)))
                                         (dissoc query-cache cache-key)
                                         query-cache)))))
      ;; cache this reaction, so it can be used to deduplicate other, later "=" subscriptions
      (swap! subscription-cache (fn [query-cache]
                                  (when debug-enabled?
                                    (when (contains? query-cache cache-key)
                                      (console :warn "re-frame: Adding a new subscription to the cache while there is an existing subscription in the cache" cache-key)))
                                  ;(console :info "ABOUT TO ASSOC , cache key: " cache-key)
                                  ;(console :info "ABOUT TO ASSOC , cache is : " query-cache)
                                  (assoc query-cache cache-key reaction)))
      (trace/merge-trace! {:tags {:reaction (reagent-id reaction)}})))
  reaction)

;; -- subscribe ---------------------------------------------------------------

(defn subscribe
  [get-handler cache-lookup get-subscription-cache
   app query]
  (assert (vector? query))
  (let [cnt (count query)]
    (assert (or (= 1 cnt) (= 2 cnt)) "Query must contain only one map")
    (if (= 2 cnt) (assert (map? (get query 1)) "Args to the query vector must be one map.")))
  (trace/with-trace {:operation (first query)
                     :op-type   :sub/create
                     :tags      {:query-v query}}
    ;(console :info (str "subs. cache-lookup: " query))
    (if-let [cached (cache-lookup app query)]
      (do
        (trace/merge-trace! {:tags {:cached?  true
                                    :reaction (reagent-id cached)}})
        ;(console :info (str "subs. returning cached " query ", " #_(pr-str cached)))
        cached)
      (let [query-id   (first query)
            ;_          (println "query id: " query-id)
            handler-fn (get-handler query-id)]
        ;(console :info "DO NOT HAVE CACHED")
        ;(console :info "handler out: " (handler-fn app query))
        ;(console :info (str "subs. computing subscription"))
        (assert handler-fn (str "Subscription handler for the following query is missing\n\n" (pr-str query-id) "\n"))

        (trace/merge-trace! {:tags {:cached? false}})
        (if (nil? handler-fn)
          (do (trace/merge-trace! {:error true})
              (console :error (str "No subscription handler registered for: " query-id "\n\nReturning a nil subscription.")))
          (do
            ;(console :info "Have handler. invoking")
            (cache-and-return! get-subscription-cache app query (handler-fn app query))))))))

;; -- reg-sub -----------------------------------------------------------------

(defn- map-vals
  "Returns a new version of 'm' in which 'f' has been applied to each value.
  (map-vals inc {:a 4, :b 2}) => {:a 5, :b 3}"
  [f m]
  (into {} (map (fn [[k v]]
                  [k (if (sequential? v) (mapv f v) (f v))])) m))

(defn map-signals
  "Runs f over signals. Signals may take several
  forms, this function handles all of them."
  [f signals]
  (cond
    (sequential? signals) (map f signals)
    (map? signals) (map-vals f signals)
    (deref? signals) (f signals)
    :else '()))

(defn to-seq
  "Coerces x to a seq if it isn't one already"
  [x]
  (cond-> x (not (sequential? x)) list))

(defn- deref-input-signals
  [signals query-id]
  (when-not ((some-fn sequential? map? deref?) signals)
    (console :error "space.matterandvoid.subscriptions: in the reg-sub for" query-id ", the input-signals function returns:" signals))
  ;(trace/merge-trace! {:tags {:input-signals (doall (to-seq (map-signals reagent-id signals)))}})
  (map-signals deref signals))

(defn make-subs-reaction
  [inputs-fn computation-fn query-id]
  (fn subs-handler-fn
    [app query-vec]
    (let [subscriptions (inputs-fn app query-vec)
          reaction-id   (atom nil)
          reaction      (make-reaction
                          (fn []
                            (trace/with-trace {:operation (first query-vec)
                                               :op-type   :sub/run
                                               :tags      {:query-v  query-vec
                                                           :reaction @reaction-id}}

                              (let [subscription-output (computation-fn (deref-input-signals subscriptions query-id) query-vec)]
                                (trace/merge-trace! {:tags {:value subscription-output}})
                                subscription-output))))]
      (reset! reaction-id (reagent-id reaction))
      reaction)))

(defn memoize-fn
  "Returns a function which is memoized, with a policy.
  For now it will retain the up to 'n' unique invocations of input to output. When buffer/cache of 'n' distinct input calls is full will
  evict the oldest first."
  ([f] (memoize-fn 100 50 f))
  ([max-args-cached-length max-history-length f]
   (let [cache_          (atom {:data         {}
                                :args-history #queue[]})
         lookup-sentinel (js-obj)]
     (fn [& args]
       (let [{:keys [args-history data]} @cache_
             v (get data args lookup-sentinel)]
         (swap! cache_
           #(cond-> %
              ;;
              (and (= (count (keys data)) max-args-cached-length)
                (not (contains? data args)))
              (update :data dissoc (peek args-history))

              (= (count args-history) max-history-length) (update :args-history pop)

              ;; cache miss
              (identical? v lookup-sentinel) (update :data assoc args (apply f args))
              true (update :args-history conj args)))
         (get (:data @cache_) args))))))

(defn reg-sub
  "db, fully qualified keyword for the query id
  optional positional args."
  [get-input-db-signal get-handler register-handler! get-subscription-cache cache-lookup
   query-id & args]
  (let [err-header (str "space.matterandvoid.subscriptions: reg-sub for " query-id ", ")
         [input-args      ;; may be empty, or one signal fn, or pairs of  :<- / vector
         computation-fn] (let [[op f :as comp-f] (take-last 2 args)]
                           (if (or (= 1 (count comp-f))
                                 (fn? op)
                                 (vector? op))
                             [(butlast args) (last args)]
                             (let [args (drop-last 2 args)]
                               (case op
                                 ;; return a function that calls the computation fn
                                 ;;  on the input signal, removing the query vector
                                 :-> [args (fn [db _] (f db))]

                                 ;; return a function that calls the computation fn
                                 ;;  on the input signal and the data in the query vector
                                 ;;  that is not the query-id
                                 :=> [args (fn [db [_ & qs]] (apply f db qs))]
                                 ;; an incorrect keyword was passed
                                 (console :error err-header "expected :-> or :=> as second to last argument, got:" op)))))
        _                       (assert (ifn? computation-fn) "Last arg should be function - your computation function.")
        memoized-computation-fn (memoize-fn 100 50 computation-fn)

        err-header              (str "space.matterandvoid.subscriptions: reg-sub for " query-id ", ")
        inputs-fn               (case (count input-args)
                                  ;; no `inputs` function provided - give the default
                                  0
                                  (do
                                    (console :info "CASE 0")
                                    (fn
                                      ([app]
                                       (println "IN case 0 args: " app)
                                       (get-input-db-signal app))
                                      ([app _] (get-input-db-signal app))))

                                  ;; a single `inputs` fn
                                  1 (let [f (first input-args)]
                                      (console :info "CASE 1")
                                      (when-not (fn? f)
                                        (console :error err-header "2nd argument expected to be an inputs function, got:" f))
                                      f)

                                  ;; one sugar pair
                                  2 (let [[marker signal-vec] input-args]
                                      (console :info "CASE 2")
                                      (when-not (= :<- marker)
                                        (console :error err-header "expected :<-, got:" marker))
                                      (fn inp-fn
                                        ([app] (subscribe get-handler cache-lookup get-subscription-cache app signal-vec))
                                        ([app _] (subscribe get-handler cache-lookup get-subscription-cache app signal-vec))))

                                  ;; multiple sugar pairs
                                  (let [pairs   (partition 2 input-args)
                                        markers (map first pairs)
                                        vecs    (map second pairs)]
                                    (console :info "CASE 3")
                                    (when-not (and (every? #{:<-} markers) (every? vector? vecs))
                                      (console :error err-header "expected pairs of :<- and vectors, got:" pairs))
                                    (fn inp-fn
                                      ([app] (map #(subscribe get-handler cache-lookup get-subscription-cache app %) vecs))
                                      ([app _] (map #(subscribe get-handler cache-lookup get-subscription-cache app %) vecs)))))]
    (console :info "registering subscription: " query-id)
    (register-handler! query-id (make-subs-reaction inputs-fn memoized-computation-fn query-id))))
