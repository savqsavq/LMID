# Load active member TXT files ----
setwd("/Volumes/Sav's 1TB/LMID_ex/Analysis/Scrapes/Active Member Scrape")

active_files <- list.files(
  pattern = "\\.txt$",
  full.names = TRUE,
  recursive = TRUE
)

library(readr)
library(dplyr)
library(tidytext)
library(stringr)
library(purrr)

# Build initial dataframe ----
active_df <- tibble(
  source = basename(active_files),
  text   = map_chr(active_files, read_file)
)

# Tokenize initial version ----
tokens_active <- active_df %>%
  unnest_tokens(word, text) %>%
  filter(!str_detect(word, "\\d")) %>% 
  anti_join(stop_words, by = "word") %>%
  mutate(word = str_squish(word)) %>%
  filter(word != "")

# Load custom SQL stopwords ----
library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "/Volumes/Sav's 1TB/LMID_ex/Analysis/Everything SQL db/lmid.db")
stop_sql <- dbGetQuery(con, "SELECT word FROM stopwords")
stop_sql <- unique(stop_sql$word)
active_stop <- stop_sql

# Reload files for consistent processing ----
active_files <- list.files(
  path = ".",
  pattern = "\\.txt$",
  recursive = TRUE,
  full.names = TRUE
)

active_df <- tibble(
  source = basename(active_files),
  text   = vapply(
    active_files,
    function(f) paste(readLines(f, warn = FALSE, encoding = "UTF-8"), collapse = " "),
    FUN.VALUE = character(1)
  )
)

# Tokenize with full cleaning ----
library(tidytext)
library(dplyr)
library(stringr)

tokens_active <- active_df %>%
  unnest_tokens(word, text) %>%
  mutate(word = str_to_lower(word)) %>%
  filter(!word %in% active_stop) %>%
  filter(word != "")

freq_active <- tokens_active %>%
  count(word, sort = TRUE)

print(freq_active, n = 250)

# Additional cleanup layers ----
library(dplyr)
library(stringr)

clean_active <- freq_active %>%
  filter(!str_detect(word, "^[0-9]+$")) %>% 
  filter(!(nchar(word) < 3))

# Archaic language ----
archaic <- c(
  "unto","ye","thou","thee","thy","thine",
  "yea","shalt","shall","hath","saith",
  "whence","whither","hence","lest",
  "whom","whose","wherefore","therefore",
  "thus","neither","therein","thereof"
)

# Narrative scaffolding verbs ----
scaffolding_verbs <- c(
  "came","come","cometh","go","went","goeth",
  "bring","brought","take","took","giveth",
  "pass","passed","forth","behold","arose",
  "saw","seen","heard","spoke","spake"
)

# Formatting artifacts ----
format_garbage <- c(
  "tg","ps","gr","chr","heb","mos","ex","isa",
  "ii","iii","iv","o",
  "words","chapter",
  "______________________________________"
)

# Names and places ----
names_places <- c(
  "moses","jacob","isaiah","jeremiah","david","solomon",
  "sam","alma","nephi","lamanites","mosiah","moroni",
  "jerusalem","egypt","israel","judah","zion"
)

# LDS phrasing fillers ----
lds_fillers <- c(
  "behold","wherefore","yea","verily","even",
  "inasmuch","insomuch","therefore"
)

# Combine all custom stopwords ----
active_stopwords <- c(
  archaic,
  scaffolding_verbs,
  format_garbage,
  names_places,
  lds_fillers
)

# Final cleaned corpus ----
clean_active <- clean_active %>%
  filter(!word %in% active_stopwords)

print(clean_active, n = 250)