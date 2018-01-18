baz <- function(x) UseMethod("baz", x)
baz.A <- function(x) "A"
baz.B <- function(x) "B"
ab <- structure(1, class = c("A", "B"))
ba <- structure(1, class = c("B", "A"))
baz(ab)
baz(ba)

baz.C <- function(x) c("C", NextMethod())
ca <- structure(1, class = c("C", "A"))
cb <- structure(1, class = c("C", "B"))
baz(ca)
baz(cb)