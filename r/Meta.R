setwd("/Volumes/SS 1TB/LMID_ex/Analysis/R Analysis/Meta-Analysis")

library(DBI)
library(RSQLite)
library(tidyverse)
library(tidytext)
library(stringr)

# Load DB ----
con <- dbConnect(SQLite(), "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db")

docs <- dbGetQuery(con, "
  SELECT id, domain, pillar, subdomain, text
  FROM documents
")

# Load stopwords + normalize ----
stop_master <- read_csv("/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/stopwords_master.csv") %>%
 mutate(
  word = str_to_lower(word),
  word = str_replace_all(word, "[\u2019\u2018]", "'")
 ) %>%
 distinct()

# Tokenize ----
tokens <- docs %>%
 mutate(text = str_replace_all(text, "[\u2019\u2018]", "'")) %>%
 unnest_tokens(word, text, token = "words") %>%
 mutate(word = str_to_lower(word)) %>%
 filter(!str_detect(word, "^[0-9]+$")) %>%
 filter(nchar(word) >= 2) %>%
 anti_join(stop_master, by = "word")

# Frequency tallies ----
freq_pillar <- tokens %>%
 count(pillar, word, sort = TRUE)

freq_domain <- tokens %>%
 count(domain, word, sort = TRUE)

freq_subdomain <- tokens %>% 
 count(subdomain, word, sort = TRUE)

# Example output tables ----
freq_pillar %>%
 group_by(pillar) %>%
 slice_max(n, n = 250) %>%
 arrange(pillar, desc(n))

freq_subdomain %>%
 group_by(subdomain) %>%
 slice_max(n, n = 50)

# bigrams ----
bigrams <- docs %>%
 mutate(text = str_replace_all(text, "[\u2019\u2018]", "'")) %>%
 unnest_tokens(bigram, text, token = "ngrams", n = 2) %>%
 separate(bigram, into = c("w1", "w2"), sep = " ") %>%
 filter(
  !str_detect(w1, "^[0-9]+$"),
  !str_detect(w2, "^[0-9]+$"),
  nchar(w1) >= 2,
  nchar(w2) >= 2
 ) %>%
 mutate(
  w1 = str_to_lower(w1),
  w2 = str_to_lower(w2)
 ) %>%
 filter(
  !w1 %in% stop_master$word,
  !w2 %in% stop_master$word
 )

bigram_counts <- bigrams %>%
 count(pillar, subdomain, w1, w2, sort = TRUE) %>%
 mutate(bigram = paste(w1, w2))

# analysis ----
bigrams <- docs %>%
 mutate(text = str_replace_all(text, "[\u2019\u2018]", "'")) %>%
 unnest_tokens(bigram, text, token = "ngrams", n = 2) %>%
 separate(bigram, into = c("w1", "w2"), sep = " ") %>%
 filter(
  !str_detect(w1, "^[0-9]+$"),
  !str_detect(w2, "^[0-9]+$"),
  nchar(w1) >= 2,
  nchar(w2) >= 2
 ) %>%
 mutate(
  w1 = str_to_lower(w1),
  w2 = str_to_lower(w2)
 ) %>%
 filter(
  !w1 %in% stop_master$word,
  !w2 %in% stop_master$word
 )

bigrams_counted <- bigrams %>%
 mutate(bigram = paste(w1, w2)) %>%
 count(pillar, subdomain, bigram, sort = TRUE)

library(ggplot2)

plot_top_bigrams <- function(df, title_text, n = 45) {
 df %>%
  ungroup() %>% 
  slice_max(order_by = n, n = n, with_ties = FALSE) %>%
  mutate(bigram = reorder(bigram, n)) %>%
  ggplot(aes(x = n, y = bigram)) +
  geom_col(fill = "#3c6faa") +
  labs(
   title = title_text,
   x = "Frequency",
   y = NULL
  ) +
  theme_minimal(base_size = 14)
}

plot_top_bigrams(
 bigrams_counted %>% filter(subdomain == "academic"),
 "Top 45 Bigrams Academic Corpus"
)

plot_top_bigrams(
 bigrams_counted %>% filter(subdomain == "general_conference"),
 "Top 45 Bigrams LDS General Conference"
)

plot_top_bigrams(
 bigrams_counted %>% filter(subdomain == "church_literature"),
 "Top 45 Bigrams Church Literature"
)

plot_top_bigrams(
 bigrams_counted %>% filter(subdomain == "mormon_stories"),
 "Top 45 Bigrams Ex-Member Narratives"
)

plot_top_bigrams(
 bigrams_counted %>% filter(subdomain == "alyssa"),
 "Top 45 Bigrams Alyssa"
)

plot_top_bigrams(
 bigrams_counted %>% filter(subdomain == "jonny"),
 "Top 45 Bigrams Jonny"
)


plot_top_words <- function(df, title_text, n = 45) {
 df %>%
  slice_max(n, n = n) %>%
  mutate(word = reorder(word, n)) %>%
  ggplot(aes(x = n, y = word)) +
  geom_col(fill = "#3c6faa") +
  labs(
   title = title_text,
   x = "Frequency",
   y = NULL
  ) +
  theme_minimal(base_size = 14)
}

plot_top_words(
 freq_subdomain %>% filter(subdomain == "academic"),
 "Top 45 Content Words Academic Corpus"
)

plot_top_words(
 freq_subdomain %>% filter(subdomain == "general_conference"),
 "Top 45 Content Words LDS General Conference"
)

plot_top_words(
 freq_subdomain %>% filter(subdomain == "church_literature"),
 "Top 45 Content Words Church Literature"
)

plot_top_words(
 freq_subdomain %>% filter(subdomain == "mormon_stories"),
 "Top 45 Content Words Ex-Member Narratives"
)

plot_top_words(
 freq_subdomain %>% filter(subdomain == "alyssa"),
 "Top 45 Content Words Alyssa"
)

plot_top_words(
 freq_subdomain %>% filter(subdomain == "jonny"),
 "Top 45 Content Words Jonny"
)