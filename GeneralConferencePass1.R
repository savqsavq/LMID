# Working directory ----
setwd("/Volumes/SS 1TB/LMID_ex/Analysis/R Analysis/Church analysis/General Conferences")

# Libraries ----
library(DBI)
library(RSQLite)
library(tidytext)
library(dplyr)
library(stringr)
library(ggplot2)
library(tibble)

# DB connections ----

# main LMID db (stopwords)
con_main <- dbConnect(SQLite(), "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid.db")
db_stop <- dbGetQuery(con_main, "SELECT word FROM stopwords")

# custom GC-specific stopwords
custom_gc_stop <- tibble(word = c(
  "ye","thou","thy","thee",
  "lord’s","god’s",
  "john","ago","days","wonderful","beloved",
  "live","lives","living","learned",
  "bring","stand"
))

# general conference db
con_gc <- dbConnect(SQLite(), "/Volumes/SS 1TB/LMID_ex/Analysis/Python Scripts/General Conference Scrape/gc.db")

# Load GC talks ----
gc_df <- dbGetQuery(con_gc, "
    SELECT id, year, month, speaker, title, text
    FROM talks
")

if (nrow(gc_df) == 0) stop("No talks found in gc.db.")

# Tokenization + cleaning ----
tokens_gc <- gc_df %>%
  unnest_tokens(word, text) %>%
  filter(word != "") %>%
  filter(!str_detect(word, '\\d')) %>%
  anti_join(stop_words, by = "word") %>%
  anti_join(db_stop,     by = "word") %>%
  anti_join(custom_gc_stop, by = "word")

# Frequency analysis ----
freq_gc <- tokens_gc %>%
  count(word, sort = TRUE)

top150_gc <- freq_gc %>% slice_max(n, n = 150)
top45_gc  <- freq_gc %>% slice_max(n, n = 45)

top150_gc
top45_gc

# Visualization ----
gc_plot <- top45_gc %>%
  mutate(word = reorder(word, n))

ggplot(gc_plot, aes(x = word, y = n)) +
  geom_col(fill = "#4b6cb7") +
  coord_flip() +
  labs(
    title = "Top 45 Words – General Conference Corpus",
    x = NULL,
    y = "Frequency"
  ) +
  theme_minimal(base_size = 14)

# Cleanup ----
dbDisconnect(con_gc)
dbDisconnect(con_main)