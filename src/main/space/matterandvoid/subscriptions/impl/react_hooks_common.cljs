(ns space.matterandvoid.subscriptions.impl.react-hooks-common
  (:require-macros [space.matterandvoid.subscriptions.impl.react-hooks-common])
  (:require
    ["react" :as react]
    ["use-sync-external-store/shim/with-selector" :refer [useSyncExternalStoreWithSelector]]
    [goog.object :as gobj]
    [space.matterandvoid.subscriptions.impl.reagent-ratom :as ratom]
    [taoensso.timbre :as log]))

(defn use-memo [f deps] (react/useMemo f deps))

;; The following was adapted from
;; https://github.com/roman01la/hooks/blob/1a98408280892da1abebde206b5ca2444aced1b3/src/hooks/impl.cljs

;; for more on implementation details see https://github.com/reactwg/react-18/discussions/86

(defn use-sync-external-store [subscribe get-snapshot]
  ;; https://reactjs.org/docs/hooks-reference.html#usesyncexternalstore
  ;; this version uses useMemo to avoid rerenders when the snapshot is the same across renders.
  (useSyncExternalStoreWithSelector
    subscribe
    get-snapshot
    get-snapshot ;; getServerSnapshot, only needed for SSR ;; todo need to test this
    identity ;; selector, not using, just returning the value itself
    =)) ;; value equality check

(defn use-run-in-reaction [reaction cleanup?]
  (let [reaction-key "reaction"
        reaction-obj (react/useRef #js{})]
    (react/useCallback
      (fn setup-subscription [listener]
        (ratom/run-in-reaction
          (fn [] (when reaction @reaction))
          (.-current reaction-obj)
          reaction-key
          listener
          {:no-cache true})
        (fn cleanup-subscription []
          (when (and cleanup? (gobj/get (.-current reaction-obj) reaction-key))
            (ratom/dispose! (gobj/get (.-current reaction-obj) reaction-key)))))
      #js [reaction])))

;; Public API

(defn use-reaction
  "Takes a Reagent Reaction and rerenders the UI component when the Reaction's value changes.
   Returns the current value of the Reaction"
  ([^clj reaction]
   (use-reaction reaction true))

  ([^clj reaction cleanup?]
   (assert (or (ratom/reaction? reaction) (ratom/cursor? reaction) (nil? reaction))
     "reaction should be an instance of reagent.ratom/Reaction or reagent.ratom/RCursor")
   (let [get-snapshot (react/useCallback (fn [] (when reaction (ratom/get-state reaction)))
                        #js[reaction])
         subscribe    (use-run-in-reaction reaction cleanup?)]
     (use-sync-external-store subscribe get-snapshot))))

;; The subscription hook uses a React Ref to wrap the Reaction.
;; The reason for doing so is so that React does not re-create the Reaction object each time the component is rendered.
;;
;; This is safe because the ref's value never changes for the lifetime of the component (per use of use-reaction)
;; Thus the caution to not read .current from a ref during rendering doesn't apply because we know it never changes.
;;
;; The guideline exists for refs whose underlying value will change between renders, but we are just using it
;; as a cache local to the component in order to not recreate the Reaction with each render.
;;
;; References:
;; - https://beta.reactjs.org/apis/react/useRef#referencing-a-value-with-a-ref
;; - https://beta.reactjs.org/apis/react/useRef#avoiding-recreating-the-ref-contents

(defn update-ref-count! [op ^clj sub]
  (set! (.-ref-count sub)
    (op (or (.-ref-count sub) 0))))

(def inc-ref-count! (partial update-ref-count! inc))
(def dec-ref-count! (partial update-ref-count! dec))

(defn cleanup-sub! [^clj sub]
  (dec-ref-count! sub)
  (when (zero? (.-ref-count sub))
    (ratom/dispose! sub)))

(defn use-sub
  "Takes a subscribe function, a datasource a subscription vector and a predicate to determine when two subscription vectors are equal.
  Runs the subscription returning its value to the calling component while also re-rendering the component when the subscription's
  value changes."
  [subscribe datasource query equal?]
  ;; We save every subscription the component uses while mounted and then dispose them all at once.
  ;; This uses simple reference counting such that if multiple components use the same subscription the subscription will
  ;; only get disposed when the last one unmounts.
  ;; This way if a query changes and a new subscription is used we don't evict that subscription from the cache until all
  ;; components that use the subscription have unmounted
  ;; The main use-case this has in mind is when arguments to a subscription are changing frequently, this avoid constantly
  ;; destroying and recreating subscriptions for a tradeoff in more memory used while the component is mounted.
  (let [last-query (react/useRef query)
        ref        (react/useRef nil)
        subs-log   (react/useRef #js[])]

    (when-not (.-current ref)
      (let [sub (ratom/in-reactive-context (subscribe datasource query))]
        (.push (.-current subs-log) sub)
        (inc-ref-count! sub)
        (set! (.-current ref) sub)))

    (when-not (equal? (.-current last-query) query)
      (set! (.-current last-query) query)
      (let [sub (ratom/in-reactive-context (subscribe datasource query))]
        (.push (.-current subs-log) sub)
        (inc-ref-count! sub)
        (set! (.-current ref) sub)))

    (react/useEffect
      (fn mount []
        (fn unmount []
          (doseq [sub (.-current subs-log)]
            (cleanup-sub! sub))))
      #js[])

    (use-reaction (.-current ref) false)))
