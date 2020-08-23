# Title     : combinereviews
# Objective : put together reviews for different companies
# Created by: xiefangzhou
# Created on: 8/23/20

library(tidyverse)

files <- fs::dir_ls("data", glob = "*.csv")

future::plan("multisession", workers = 20)

data <- furrr::future_map(files,
                          function(x) {
                            id <- str_extract(x, "[0-9]+")
                            data <- read_csv(x, col_types = cols(.default = "c"))
                            data <- data %>%
                              mutate(id = !!id) %>%
                              relocate(id, .before = everything())
                            data
                          }) %>%
  bind_rows()

# vroom offers faster data reading than readr
#data <- vroom::vroom(files)
vroom::vroom_write(data, "reviews_education.csv")
