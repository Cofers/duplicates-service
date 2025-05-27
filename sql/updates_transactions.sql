CREATE TABLE IF NOT EXISTS `production-400914.data_duplicates.updates_transactions` (
  `original_checksum` STRING NOT NULL,
  `new_checksum` STRING NOT NULL,
  `levenshtein_distance` FLOAT64,
  `cosine_similarity` FLOAT64,
  `jaro_winkler_similarity` FLOAT64,
  `account_number` STRING NOT NULL,
  `bank` STRING NOT NULL,
  `company_id` STRING NOT NULL,
  `date` DATE NOT NULL
)
PARTITION BY date
CLUSTER BY company_id, bank, account_number; 