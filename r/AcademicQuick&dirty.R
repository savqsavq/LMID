# Load academic TXT files ----
setwd("/Volumes/Sav's 1TB/LMID_ex/R Analysis/Academic Corpus txt")

files <- list.files(pattern = "\\.txt$", full.names = TRUE)

texts <- lapply(files, function(f) {
  paste(readLines(f, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
})

names(texts) <- basename(files)

library(tidytext)
library(dplyr)
library(stringr)

# Build master tibble ----
academic_df <- tibble(
  source = names(texts),
  text = unlist(texts)
)

# Tokenize and clean ----
tokens <- academic_df %>%
  unnest_tokens(word, text) %>%
  anti_join(stop_words, by = "word") %>%
  mutate(word = str_trim(word)) %>%
  filter(word != "")

freq <- tokens %>%
  count(word, sort = TRUE)

head(freq, 50)

library(dplyr)
library(ggplot2)

# Top 45 (initial) ----
academic_top45 <- freq_clean2 %>%
  slice_max(order_by = n, n = 45) %>%
  mutate(word = reorder(word, n))

ggplot(academic_top45, aes(x = word, y = n)) +
  geom_col(fill = "#4b6cb7") +
  coord_flip() +
  labs(
    title = "Top 45 Content Words in Academic Corpus",
    x = NULL,
    y = "Frequency"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title = element_text(face = "bold", size = 16),
    axis.text.y = element_text(size = 11)
  )

# Load DB stopwords ----
library(DBI)
library(RSQLite)
library(tidytext)
library(dplyr)
library(stringr)

con <- dbConnect(SQLite(), "/Volumes/Sav's 1TB/LMID_ex/Python Analysis/db/lmid.db")
db_stopwords <- dbGetQuery(con, "SELECT word FROM stopwords")

# Re-read academic txt ----
setwd("/Volumes/Sav's 1TB/LMID_ex/R Analysis/Academic Corpus txt")
files <- list.files(pattern = "\\.txt$", full.names = TRUE)

texts <- lapply(files, function(f) {
  paste(readLines(f, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
})
names(texts) <- basename(files)

# Build tibble again ----
academic_df <- tibble(
  source = names(texts),
  text = unlist(texts)
)

# Tokenize + full cleaning ----
tokens <- academic_df %>%
  unnest_tokens(word, text) %>%
  mutate(word = str_trim(word)) %>%
  filter(word != "") %>%
  filter(!str_detect(word, "\\d")) %>%
  anti_join(stop_words, by = "word") %>%
  anti_join(db_stopwords, by = "word")

freq <- tokens %>% count(word, sort = TRUE)
print(freq, n = 150)

# Top 45 (final) ----
library(dplyr)
library(ggplot2)

academic_top45 <- freq %>%
  slice_max(order_by = n, n = 45) %>%
  mutate(word = reorder(word, n))

ggplot(academic_top45, aes(x = word, y = n)) +
  geom_col(fill = "#4b6cb7") +
  coord_flip() +
  labs(
    title = "Top 45 Content Words in Academic Corpus",
    x = NULL,
    y = "Frequency"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title = element_text(face = "bold", size = 16),
    axis.text.y = element_text(size = 11),
    panel.grid.minor = element_blank()
  )