args <- grep("^--file=", commandArgs(), value = TRUE)
root <- dirname(dirname(normalizePath(sub("^--file=", "", args[[1]]))))
Sys.setenv(CAMS_TEST_PROJECT_DIR = root)
testthat::test_dir(file.path(root, "tests"), reporter = "summary", stop_on_failure = TRUE)
