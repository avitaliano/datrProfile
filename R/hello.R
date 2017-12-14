# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

hello <- function() {
  print("Hello, world!")
}

# setup test
#devtools::use_testthat()
devtools::use_package("snow")
devtools::use_package("odbc")
devtools::use_package("dplyr")

# Google's R Style Guide
# https://google.github.io/styleguide/Rguide.xml

# checks code style
# lintr::lint_package()
#devtools::load_all()
