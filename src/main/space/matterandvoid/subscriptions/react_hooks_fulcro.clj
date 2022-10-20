(ns space.matterandvoid.subscriptions.react-hooks-fulcro
  (:require [space.matterandvoid.subscriptions.fulcro :as subs]))

(defmacro use-sub-memo
  "Macro that expands expands to `use-sub`, memoizes the subscription vector so that the underlying subscription
  is reused across re-renders by React. If your subscription vector contains an arguments map, it is memoized with dependencies
  being the values of the map. If you pass a symbol as the arguments the symbol will be used as the dependency for useMemo
  thus, you are expected to memoize the arguments yourself."
  ([datasource sub-vector]
   (let [sub (when (vector? sub-vector) (first sub-vector))
         args (when (vector? sub-vector) (second sub-vector))
         memo-val (-> args meta :memo)
         equal?   (or memo-val 'cljs.core/identical?)]
     (if (map? args)
       (let [map-vals (vals args)]
         (if (false? memo-val)
           `(use-sub ~[sub args])
           `(let [memo-query# (react/useMemo (fn [] ~[sub args]) (cljs.core/array ~@map-vals))]
              (use-sub ~datasource memo-query# ~equal?))))

       (cond
         (symbol? sub-vector)
         `(use-sub ~datasource ~sub-vector ~equal?)

         (nil? args)
         `(let [memo-query# (react/useMemo (fn [] [~sub]) (cljs.core/array))]
            (use-sub ~datasource memo-query# ~equal?))

         :else
         `(let [memo-query# (react/useMemo (fn [] ~[sub args]) (cljs.core/array ~args))]
            (use-sub ~datasource memo-query# ~equal?))))))
  ([args]
   `(use-sub-memo (react/useContext subs/datasource-context) ~args)))

(defmacro use-sub-map
  "A react hook that subscribes to multiple subscriptions, the return value of the hook is the return value of the
  subscriptions which will cause the consuming react function component to update when the subscriptions' values update.

  Takes an optional data source (fulcro application) and a hashmap
  - keys are keywords (qualified or simple) that you make up.
  - values are subscription vectors.
  Returns a map with the same keys and the values are the subscriptions subscribed and deref'd (thus, being their current values).

  The single-arity version takes only a query map and will use the suscription datasource-context to read the fulcro app from
  React context."
  ([query-map]
   (assert (map? query-map) "You must pass a map literal to use-sub-map")
   (let [datasource-sym (gensym "datasource")]
     `(let [~datasource-sym (react/useContext subs/datasource-context)]
        ~(->> query-map
           (map (fn [[k query]]
                  `[~k (use-sub-memo ~datasource-sym ~query)]))
           (into {})))))

  ([datasource query-map]
   (assert (map? query-map) "You must pass a map literal to use-sub-map")
   (->> query-map (map (fn [[k query]] `[~k (use-sub-memo ~datasource ~query)])) (into {}))))
