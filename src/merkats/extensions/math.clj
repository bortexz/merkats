(ns merkats.extensions.math
  (:import (java.math RoundingMode)))

(defn round-step
  "Given BigDecimal `value`, BigDecimal `step` and RoundingMode `rm`: 
   Rounds the given value to the closer step, setting the scale of the resulting decimal
   to same scale as step, using rm (by default RoundingMode/HALF_DOWN).
   Note: RoundingMode/HALF_EVEN is not supported, as both edges of a step could be even or odd.
   `step` should be a positive decimal."
  (^BigDecimal [^BigDecimal value ^BigDecimal step]
   (round-step value step RoundingMode/HALF_DOWN))
  (^BigDecimal [^BigDecimal value ^BigDecimal step ^RoundingMode rm]
   (let [^BigDecimal r (.remainder value step)
         rv (if (zero? r)
              value
              (case (.name rm)
                "UP"
                (.add (.subtract value r)
                      (.multiply step (BigDecimal/valueOf ^int (.signum value))))

                "DOWN"
                (.subtract value r)

                "HALF_UP"
                (if (>= (.divide (.abs r) step) 0.5M)
                  (.add (.subtract value r)
                        (.multiply step (BigDecimal/valueOf ^int (.signum value))))
                  (.subtract value r))

                "HALF_DOWN"
                (if (> (.divide (.abs r) step) 0.5M)
                  (.add (.subtract value r)
                        (.multiply step (BigDecimal/valueOf ^int (.signum value))))
                  (.subtract value r))

                "CEILING"
                (case (.signum value)
                  1  (.add (.subtract value r) step)
                  -1 (.subtract value r))

                "FLOOR"
                (case (.signum value)
                  1 (.subtract value r)
                  -1 (.add (.subtract value r) (.negate step)))))]
     (.setScale rv (.scale step)))))
