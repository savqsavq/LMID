# Working directory ----
setwd("/Volumes/SS 1TB/LMID_ex/Analysis/R Analysis/Meta-Analysis")

# Libraries ----
library(DBI)
library(RSQLite)
library(tidyverse)
library(tidytext)
library(stringr)
library(ggplot2)

# DB connect ----
con <- dbConnect(
  SQLite(),
  "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/lmid_master.db"
)

# Master stopwords ----
stop_master <- read_csv(
  "/Volumes/SS 1TB/LMID_ex/Analysis/Everything SQL db/stopwords_master.csv"
) %>%
  mutate(
    word = str_to_lower(word),
    word = str_replace_all(word, "[\u2019\u2018]", "'")
  ) %>%
  distinct()

# Piece loader ----
load_pieces <- function(domain = NULL, subdomain = NULL, piece_size = 50000) {
  
  base <- "SELECT id, text FROM documents WHERE 1=1"
  if (!is.null(domain))    base <- paste0(base, sprintf(" AND domain = '%s'", domain))
  if (!is.null(subdomain)) base <- paste0(base, sprintf(" AND subdomain = '%s'", subdomain))
  
  offset <- 0
  
  repeat {
    q <- sprintf("%s LIMIT %d OFFSET %d", base, piece_size, offset)
    df <- dbGetQuery(con, q)
    
    if (nrow(df) == 0) break
    yield(df)
    
    offset <- offset + piece_size
  }
}

# Text cleaning ----
clean_text <- function(df) {
  df %>%
    mutate(text = str_replace_all(text, "[\u2019\u2018]", "'"))
}

# Unigrams ----
tokenize_unigrams <- function(df) {
  df %>%
    unnest_tokens(word, text, token = "words") %>%
    mutate(word = str_to_lower(word)) %>%
    filter(
      !str_detect(word, "^[0-9]+$"),
      nchar(word) >= 2
    ) %>%
    anti_join(stop_master, by = "word")
}

# Bigrams ----
tokenize_bigrams <- function(df) {
  df %>%
    unnest_tokens(bigram, text, token = "ngrams", n = 2) %>%
    separate(bigram, into = c("w1", "w2"), sep = " ") %>%
    mutate(
      w1 = str_to_lower(w1),
      w2 = str_to_lower(w2)
    ) %>%
    filter(
      nchar(w1) >= 2, nchar(w2) >= 2,
      !w1 %in% stop_master$word,
      !w2 %in% stop_master$word,
      !str_detect(w1, "^[0-9]+$"),
      !str_detect(w2, "^[0-9]+$")
    ) %>%
    mutate(bigram = paste(w1, w2))
}

# Unigram analysis R runner ----
run_unigram_analysis <- function(domain = NULL, subdomain = NULL) {
  
  total_uni <- tibble(word = character(), n = integer())
  
  offset <- 0
  piece_size <- 50000
  
  repeat {
    q <- "SELECT id, text FROM documents WHERE 1=1"
    if (!is.null(domain))    q <- paste0(q, sprintf(" AND domain='%s'", domain))
    if (!is.null(subdomain)) q <- paste0(q, sprintf(" AND subdomain='%s'", subdomain))
    q <- sprintf("%s LIMIT %d OFFSET %d", q, piece_size, offset)
    
    df <- dbGetQuery(con, q)
    if (nrow(df) == 0) break
    
    df <- clean_text(df)
    u <- tokenize_unigrams(df)
    
    total_uni <- bind_rows(total_uni, count(u, word))
    
    offset <- offset + piece_size
  }
  
  total_uni %>%
    group_by(word) %>%
    summarize(n = sum(n), .groups = "drop") %>%
    arrange(desc(n))
}

# Bigram analysis ----
run_bigram_analysis <- function(domain = NULL, subdomain = NULL) {
  
  total_bi <- tibble(bigram = character(), n = integer())
  
  offset <- 0
  piece_size <- 50000
  
  repeat {
    q <- "SELECT id, text FROM documents WHERE 1=1"
    if (!is.null(domain))    q <- paste0(q, sprintf(" AND domain='%s'", domain))
    if (!is.null(subdomain)) q <- paste0(q, sprintf(" AND subdomain='%s'", subdomain))
    q <- sprintf("%s LIMIT %d OFFSET %d", q, piece_size, offset)
    
    df <- dbGetQuery(con, q)
    if (nrow(df) == 0) break
    
    df <- clean_text(df)
    b <- tokenize_bigrams(df)
    
    total_bi <- bind_rows(total_bi, count(b, bigram))
    
    offset <- offset + piece_size
  }
  
  total_bi %>%
    group_by(bigram) %>%
    summarize(n = sum(n), .groups = "drop") %>%
    arrange(desc(n))
}

# Plot helpers ----
plot_top_words <- function(df, title_text, n = 40) {
  df %>%
    slice_max(n, n = n) %>%
    mutate(word = reorder(word, n)) %>%
    ggplot(aes(x = n, y = word)) +
    geom_col(fill = "#3c6faa") +
    labs(title = title_text, x = "Frequency", y = NULL) +
    theme_minimal(base_size = 14)
}

plot_top_bigrams <- function(df, title_text, n = 40) {
  df %>%
    slice_max(n, n = n) %>%
    mutate(bigram = reorder(bigram, n)) %>%
    ggplot(aes(x = n, y = bigram)) +
    geom_col(fill = "#3c6faa") +
    labs(title = title_text, x = "Frequency", y = NULL) +
    theme_minimal(base_size = 14)
}

# Domain-level runs ----
church_uni <- run_unigram_analysis("church")
active_uni <- run_unigram_analysis("active_member")
ex_uni     <- run_unigram_analysis("ex_member")

plot_top_words(church_uni, "CHURCH — Top Unigrams")
plot_top_words(active_uni, "ACTIVE MEMBERS — Top Unigrams")
plot_top_words(ex_uni, "EX-MEMBERS — Top Unigrams")

church_bi <- run_bigram_analysis("church")
active_bi <- run_bigram_analysis("active_member")
ex_bi     <- run_bigram_analysis("ex_member")

plot_top_bigrams(church_bi, "CHURCH — Top Bigrams")
plot_top_bigrams(active_bi, "ACTIVE MEMBERS — Top Bigrams")
plot_top_bigrams(ex_bi, "EX-MEMBERS — Top Bigrams")


# Subdomain meta-analysis PDFs ----
library(gridExtra)

subdomains <- c(
  "mormon_stories",
  "alyssa",
  "dont_miss_this",
  "saints_unscripted",
  "lds_living",
  "general_conference",
  "byu_non_ga",
  "literature",
  "byu_ga",
  "academic",
  "jonny"
)

# Output folders ----
dir.create("Outputs", showWarnings = FALSE)
dir.create("Outputs/unigrams", showWarnings = FALSE)
dir.create("Outputs/bigrams",  showWarnings = FALSE)
dir.create("Outputs/combined", showWarnings = FALSE)

# Runner ----
for (s in subdomains) {
  
  message(sprintf("Processing %s ...", s))
  
  uni <- run_unigram_analysis(subdomain = s)
  bi  <- run_bigram_analysis(subdomain = s)
  
  p1 <- plot_top_words(uni, paste(s, "— Top Unigrams"))
  p2 <- plot_top_bigrams(bi, paste(s, "— Top Bigrams"))
  
  pdf(sprintf("Outputs/unigrams/%s_unigrams.pdf", s), width = 8, height = 11.5)
  print(p1)
  dev.off()
  
  pdf(sprintf("Outputs/bigrams/%s_bigrams.pdf", s), width = 8, height = 11.5)
  print(p2)
  dev.off()
  
  pdf(sprintf("Outputs/combined/%s_combined.pdf", s), width = 8, height = 11.5)
  grid.arrange(p1, p2, ncol = 1)
  dev.off()
}

message("Subdomain PDFs complete.")

# Manual review printer ----
show_top <- function(df, n = 50) {
  df %>% slice_max(n, n = n)
}

for (s in subdomains) {
  cat("\n\n------------------------------\n")
  cat("SUBDOMAIN:", s, "\n")
  cat("------------------------------\n\n")
  
  uni <- run_unigram_analysis(subdomain = s)
  bi  <- run_bigram_analysis(subdomain = s)
  
  cat("TOP 50 UNIGRAMS:\n")
  print(show_top(uni, 50))
  
  cat("\nTOP 50 BIGRAMS:\n")
  print(show_top(bi, 50))
}