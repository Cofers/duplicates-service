CREATE TABLE IF NOT EXISTS `production-400914.cofers_data_silver.analyze_transactions` (
  `checksum_old` STRING NOT NULL,
  `checksum_new` STRING NOT NULL,
  `account_number` STRING NOT NULL,
  `bank` STRING NOT NULL,
  `company_id` STRING NOT NULL,
  `transaction_date` STRING NOT NULL,
  `type_of_conflict` STRING NOT NULL,
  `mosaic_reason` STRING NOT NULL,
  `date` date NOT NULL
)
PARTITION BY date
CLUSTER BY company_id, bank, account_number; 