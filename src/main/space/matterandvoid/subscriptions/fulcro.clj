(ns space.matterandvoid.subscriptions.fulcro
  (:require
    [com.fulcrologic.fulcro.application :as fulcro.app]
    [space.matterandvoid.subscriptions :as-alias subs-keys]
    [space.matterandvoid.subscriptions.impl.fulcro :as impl]
    [space.matterandvoid.subscriptions.impl.reagent-ratom :as ratom]))

(def query-key ::subs-keys/query)
(defn set-memoize-fn! [f] (impl/set-memoize-fn! f))
(defn set-args-merge-fn! [f] (impl/set-args-merge-fn! f))

(defn reg-sub
  "A call to `reg-sub` associates a `query-id` with two functions ->
  a function returning input signals and a function (the signals function)
  taking the input-signals current value(s) as input and returning a value (the computation function).

  The two functions provide 'a mechanism' for creating a node
  in the Signal Graph. When a node of type `query-id` is needed,
  the two functions can be used to create it.

  The three arguments are:

  - `query-id` - typically a namespaced keyword (later used in subscribe)
  - optionally, an `input signals` function which returns the input data
    flows required by this kind of node.
  - a `computation function` which computes the value (output) of the
    node (from the input data flows)

  It registers 'a mechanism' (the two functions) by which nodes
  can be created later, when a node is bought into existence by the
  use of `subscribe` in a `View Function`reg-sub.

  The `computation function` is expected to take two arguments:

    - `input-values` - the values which flow into this node (how is it wired into the graph?)
    - `query-vector` - the vector given to `subscribe`

  and it returns a computed value (which then becomes the output of the node)

  When `computation function` is called, the 2nd `query-vector` argument will be that
  vector supplied to the `subscribe`. So, if the call was `(subscribe [:sub-id 3 :blue])`,
  then the `query-vector` supplied to the computaton function will be `[:sub-id 3 :blue]`.

  The argument(s) supplied to `reg-sub` between `query-id` and the `computation-function`
  can vary in 3 ways, but whatever is there defines the `input signals` part
  of `the mechanism`, specifying what input values \"flow into\" the
  `computation function` (as the 1st argument) when it is called."
  [query-id & args]
  (apply impl/reg-sub query-id args))

(defn subscribe
  "Given a `query` vector, returns a Reagent `reaction` which will, over
  time, reactively deliver a stream of values. Also known as a `Signal`.

  To obtain the current value from the Signal, it must be dereferenced"
  [?app query] (impl/subscribe ?app query))

(defn <sub
  "Subscribe and deref a subscription, returning its value, not a reaction."
  [?app query]
  (impl/<sub ?app query))

(defn clear-sub
  "Unregisters subscription handlers (presumably registered previously via the use of `reg-sub`).

  When called with no args, it will unregister all currently registered subscription handlers.

  When given one arg, assumed to be the `id` of a previously registered
  subscription handler, it will unregister the associated handler. Will produce a warning to
  console if it finds no matching registration.

  NOTE: Depending on the usecase, it may be necessary to call `clear-subscription-cache!` afterwards"
  {:api-docs/heading "Subscriptions"}
  ([registry] (impl/clear-handlers registry))
  ([registry query-id] (impl/clear-handlers registry query-id)))

(defn reg-sub-raw
  "This is a low level, advanced function.  You should probably be
  using `reg-sub` instead.

  Some explanation is available in the docs at
  <a href=\"http://day8.github.io/re-frame/flow-mechanics/\" target=\"_blank\">http://day8.github.io/re-frame/flow-mechanics/</a>"
  {:api-docs/heading "Subscriptions"}
  [query-id handler-fn] (impl/reg-sub-raw query-id handler-fn))

(defn clear-subscription-cache!
  "Removes all subscriptions from the cache.

  This function can be used at development time or test time. Useful when hot reloading
  namespaces containing subscription handlers. Also call it after a React/render exception,
  because React components won't have been cleaned up properly. And this, in turn, means
  the subscriptions within those components won't have been cleaned up correctly. So this
  forces the issue."
  [registry] (impl/clear-subscription-cache! registry))

(defn clear-handlers
  ([app] (impl/clear-handlers app))
  ([app id] (impl/clear-handlers app id)))

(defn with-headless-fulcro
  "Takes a fulcro app, disables all UI rendering and replaces the state atom with a Reagent RAtom."
  [app]
  (assoc app :render-root! identity
             :optimized-render! identity
             :hydrate-root! identity
             ::fulcro.app/state-atom (ratom/atom @(::fulcro.app/state-atom app))))

(defmacro defregsub
  "Has the same function signature as `reg-sub`.
  Registers a subscription and creates a function which is invokes subscribe and deref on the registered subscription
  with the args map passed in."
  [sub-name & args]
  (let [sub-kw (keyword (str *ns*) (str sub-name))]
    `(do
       (reg-sub ~sub-kw ~@args)

       (defn ~sub-name
         ([app#] (deref (subscribe app# [~sub-kw])))
         ([app# args#] (deref (subscribe app# [~sub-kw args#])))))))

(defn make-sub-fn [query-id sub-args]
  (impl/make-sub-fn ::subscription query-id sub-args))

(defmacro defsub
  "Has the same function signature as `reg-sub` without a keyword name argument.
  Returns a subscription function and creates a function which invokes subscribe and deref on the registered subscription
  with the args map passed in."
  [fn-name & args]
  `(def ~fn-name (make-sub-fn ~(keyword (str *ns*) (str fn-name)) ~(vec args))))

(defmacro defsubraw
  "Creates a subscription function that takes the datasource ratom and optionally an args map and returns a Reaction.
  Takes a subscription name args vector and body - the body will be wrapped in a function

  (defsubraw [db-ratom_ ?opts-map]
     (,, your code) (forms here))"
  [sub-name args & body]
  `(impl/defsubraw ::subscription ~sub-name ~args ~body))

(defmacro deflayer2-sub
  "Takes a symbol for a subscription name and a way to derive a path in your fulcro app db. Returns a function subscription
  which itself returns a Reagent RCursor.
  Supports a vector path, a single keyword, or a function which takes the RAtom datasource and the arguments map passed to subscribe and
  must return a path vector to use as an RCursor path.

  Examples:

  (deflayer2-sub my-subscription :a-path-in-your-db)

  (deflayer2-sub my-subscription [:a-path-in-your-db])

  (deflayer2-sub my-subscription (fn [db-atom sub-args-map] [:a-key (:some-val sub-args-map])))
  "
  [sub-name ?path] `(impl/deflayer2-sub ::subscription ~sub-name ~?path))

(defmacro defsub-indirect-lookup
  "Takes a keyword and a subscription that returns an id
  Deals with the pattern where a layer-2 subscription's ID value is itself resolved via a subscription.

  expands to:

  (defsubraw <sub-name>
    [db_]
    (get-in @db_ [kw (id-sub db_)]))"
  [sub-name kw id-sub]
  `(defsubraw ~sub-name
     [db_#]
     (get-in @db_# [~kw (~id-sub db_#)])))

(defn sub-fn
  "Takes a function that returns either a Reaction or RCursor. Returns a function that when invoked delegates to `f` and
   derefs its output. The returned function can be used in subscriptions."
  [f]
  (impl/sub-fn ::subscription f))

(defn with-name
  [f sub-name]
  (vary-meta f assoc ::sub-name sub-name))
