(ns merkats.extensions.spec)

(defmacro def
  "Macro to put docs on specs. The docs are not stored anywhere, but they can be introduced on s/def
   after the keyword and before the spec."
  [kw & [doc spec]]
  (let [spec (if (and doc spec)
               spec
               doc)]
    `(clojure.alpha.spec/def ~kw ~spec)))
