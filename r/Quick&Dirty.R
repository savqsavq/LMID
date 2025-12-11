setwd("/Volumes/Sav's 1TB/LMID_ex/R Analysis")
install.packages(c("DBI", "RSQLite", "tidytext", "dplyr", "ggplot2"))

library(DBI)
library(RSQLite)
library(tidytext)
library(dplyr)
library(ggplot2)

# Connect to DB ----
con <- dbConnect(SQLite(), "/Volumes/Sav's 1TB/LMID_ex/Python Analysis/db/lmid.db")
dbListTables(con)

alyssa <- dbGetQuery(con, "SELECT text FROM alyssa_transcripts;")
johnny <- dbGetQuery(con, "SELECT text FROM jonny_transcripts;")

stop_tbl <- dbGetQuery(con, "SELECT word FROM stopwords;")
stop_words2 <- tibble(word = stop_tbl$word)

head(alyssa_tokens, 50)
alyssa_tokens %>% filter(grepl("'", word)) %>% head(50)

# Token normalization ----
normalize_tokens <- function(df) {
  df %>%
    mutate(word = gsub("’", "'", word)) %>%
    mutate(word = gsub("'s$", "", word)) %>%
    mutate(word = gsub("s'$", "s", word)) %>%
    mutate(word = gsub("^'", "", word)) %>%
    mutate(word = gsub("'$", "", word))
}

# Alyssa tokens ----
alyssa_tokens <- alyssa %>%
  unnest_tokens(word, text) %>%
  normalize_tokens() %>%
  anti_join(stop_words2, by = "word") %>%
  count(word, sort = TRUE)

# Johnny tokens ----
johnny_tokens <- johnny %>%
  unnest_tokens(word, text) %>%
  normalize_tokens() %>%
  anti_join(stop_words2, by = "word") %>%
  count(word, sort = TRUE)

# Top 45 word plots ----
alyssa_tokens %>%
  slice_max(n, n = 45) %>%
  ggplot(aes(x = reorder(word, n), y = n)) +
  geom_col(fill = "black") +
  coord_flip() +
  labs(
    title = "Alyssa Top 45 Content Words",
    x = "Word",
    y = "Frequency"
  )

johnny_tokens %>%
  slice_max(n, n = 45) %>%
  ggplot(aes(x = reorder(word, n), y = n)) +
  geom_col(fill = "black") +
  coord_flip() +
  labs(
    title = "Johnny Top 45 Content Words",
    x = "Word",
    y = "Frequency"
  )

# Combine both creators ----
alyssa_tokens$creator <- "alyssa"
johnny_tokens$creator <- "johnny"

combined <- bind_rows(alyssa_tokens, johnny_tokens)

combined <- combined %>%
  group_by(creator) %>%
  mutate(freq_norm = n / sum(n))

install.packages("tidyr")
install.packages("tidyverse")
library(tidyr)

pivot <- combined %>%
  select(creator, word, freq_norm) %>%
  pivot_wider(names_from = creator, values_from = freq_norm, values_fill = 0)

pivot <- pivot %>%
  mutate(diff = alyssa - johnny)

# Combined 45 words ----
combined_tokens <- bind_rows(
  alyssa_tokens %>% mutate(creator = "alyssa"),
  johnny_tokens %>% mutate(creator = "johnny")
)

combined_totals <- combined_tokens %>%
  group_by(word) %>%
  summarise(n = sum(n), .groups = "drop") %>%
  arrange(desc(n))

top45_combined <- combined_totals %>%
  slice_max(n, n = 45)

# Plot combined top 45 ----
ggplot(top45_combined, aes(x = reorder(word, n), y = n)) +
  geom_col(fill = "black") +
  coord_flip() +
  labs(
    title = "Combined Top 45 Content Words (Alyssa + Johnny)",
    x = "Word",
    y = "Frequency"
  )

# Relative emphasis calculations ----
top45_words <- top45_combined$word

creator_contrib <- combined_tokens %>%
  filter(word %in% top45_words) %>%
  group_by(word, creator) %>%
  summarise(n = sum(n), .groups = "drop")

creator_contrib <- creator_contrib %>%
  group_by(word) %>%
  mutate(total = sum(n), prop = n / total)

alyssa_norm <- alyssa_tokens %>%
  mutate(freq_norm = n / sum(n)) %>%
  select(word, alyssa_norm = freq_norm)

johnny_norm <- johnny_tokens %>%
  mutate(freq_norm = n / sum(n)) %>%
  select(word, johnny_norm = freq_norm)

merged <- full_join(alyssa_norm, johnny_norm, by = "word") %>%
  replace_na(list(alyssa_norm = 0, johnny_norm = 0)) %>%
  mutate(diff = alyssa_norm - johnny_norm)

plotdata <- merged %>%
  filter(word %in% top45)

# Relative emphasis plot ----
ggplot(plotdata, aes(x = reorder(word, diff), y = diff)) +
  geom_col(fill = "black") +
  coord_flip() +
  labs(
    title = "Relative Emphasis of Top 45 Words (Alyssa vs Johnny)",
    x = "Word",
    y = "Alyssa - Johnny (Normalized Frequency)"
  )

# Creator-dominant words ----
alyssa_dom <- plotdata %>%
  filter(diff > 0) %>%
  arrange(desc(diff)) %>%
  slice_head(n = 20)

johnny_dom <- plotdata %>%
  filter(diff < 0) %>%
  arrange(diff) %>%
  slice_head(n = 20)

ggplot(alyssa_dom, aes(x = reorder(word, diff), y = diff)) +
  geom_col(fill = "black") +
  coord_flip() +
  labs(
    title = "Words Alyssa Emphasizes More",
    x = "Word",
    y = "Normalized Difference"
  )

ggplot(johnny_dom, aes(x = reorder(word, diff), y = diff)) +
  geom_col(fill = "gray40") +
  coord_flip() +
  labs(
    title = "Words Johnny Emphasizes More",
    x = "Word",
    y = "Normalized Difference"
  )