import pandas as pd
import pandas_gbq
import re
from collections import Counter
import math
import time
from functools import wraps
import logging
import warnings
from Levenshtein import distance as levenshtein_distance
import jellyfish


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable progress bar in pandas_gbq
pandas_gbq.context.progress_bar_type = None

# Configure BigQuery region
pandas_gbq.context.project = "production-400914"
pandas_gbq.context.location = "us-east1"

# Suppress specific UserWarning from google-cloud-bigquery
warnings.filterwarnings(
    'ignore',
    category=UserWarning,
    module='google.cloud.bigquery.table'
)

def measure_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        total_time = time.time() - start
        logger.info(f"Execution time {func.__name__}: {total_time:.2f} seconds")
        return result

    return wrapper


class TransactionUpdateDetector:
    def __init__(self, project_id: str):
        """Initialize the TransactionUpdateDetector with project configuration."""
        self.silver_data = None
        self.new_ingestion = None
        self.project_id = project_id
        self.levenshtein_threshold = 3
        self.cosine_threshold = 0.8
        self.jaro_winkler_threshold = 0.9
        self.amount_tolerance = 0.01

    def _load_data(
        self, silver_data: pd.DataFrame, new_ingestion: pd.DataFrame
    ) -> None:
        """Load and validate input dataframes."""
        required_columns = [
            "account_number",
            "concept",
            "amount",
            "transaction_date",
            "extraction_date",
        ]
        for df in [silver_data, new_ingestion]:
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing columns: {missing_cols}")
        self.silver_data = silver_data.copy()
        self.new_ingestion = new_ingestion.copy()

    @measure_time
    def _load_silver_data_from_bigquery(
        self, 
        transaction_date: str,
        account_number: str,
        company_id: str,
        bank: str
    ) -> None:
        """Load historical transactions from BigQuery for comparison."""
        query = f"""
        SELECT DISTINCT
            checksum,
            account_number,
            concept,
            amount,
            transaction_date,
            extraction_date,
            company_id,
            bank
        FROM `{self.project_id}.cofers_data_silver.transactions`
        WHERE DATE(transaction_date) = '{transaction_date}'
        AND account_number = '{account_number}'
        AND company_id = '{company_id}'
        AND bank = '{bank}'
        """
        logger.debug("BigQuery query: %s", query)

        self.silver_data = pandas_gbq.read_gbq(
            query, 
            project_id=self.project_id,
            location="us-east1"
        )

    def _normalize_text(self, text):
        """Normalize text for comparison."""
        if pd.isna(text):
            return ""
        text = str(text).lower()
        text = re.sub(r"[^\w\s]", "", text)
        return text

    def _compare_amounts(self, amount1, amount2):
        """Compare two amounts within tolerance threshold."""
        if pd.isna(amount1) or pd.isna(amount2):
            return False
        return abs(float(amount1) - float(amount2)) <= self.amount_tolerance

    def _cosine_similarity(self, text1, text2):
        """Calculate cosine similarity between two texts."""
        text1 = self._normalize_text(text1)
        text2 = self._normalize_text(text2)

        words1 = text1.split()
        words2 = text2.split()

        vector1 = Counter(words1)
        vector2 = Counter(words2)

        intersection = set(vector1.keys()) & set(vector2.keys())
        numerator = sum([vector1[x] * vector2[x] for x in intersection])

        sum1 = sum([vector1[x] ** 2 for x in vector1.keys()])
        sum2 = sum([vector2[x] ** 2 for x in vector2.keys()])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)

        if not denominator:
            return 0.0
        return float(numerator) / denominator

    def _jaro_winkler_similarity(self, s1, s2):
        """Calculate Jaro-Winkler similarity between two strings using jellyfish."""
        s1 = str(s1)
        s2 = str(s2)
        return jellyfish.jaro_winkler_similarity(s1, s2)

    def detect_updates(
        self, 
        transaction: dict,
        transaction_date: str,
        account_number: str,
        company_id: str,
        bank: str
    ) -> pd.DataFrame:
        """Detect potential updates in new transaction compared to silver data."""
        # Load silver data for the same day, account, company and bank
        self._load_silver_data_from_bigquery(
            transaction_date=transaction_date,
            account_number=account_number,
            company_id=company_id,
            bank=bank
        )

        if self.silver_data.empty:
            logger.info("No historical transactions found for comparison.")
            return pd.DataFrame()

        # Convert new transaction to DataFrame
        self.new_ingestion = pd.DataFrame([transaction])

        # Convert 'amount' column to float in both DataFrames
        self.new_ingestion["amount"] = pd.to_numeric(
            self.new_ingestion["amount"], errors="coerce"
        )
        self.silver_data["amount"] = pd.to_numeric(
            self.silver_data["amount"], errors="coerce"
        )

        # Convert dates
        self.new_ingestion["transaction_date"] = pd.to_datetime(
            self.new_ingestion["transaction_date"]
        ).dt.date
        self.silver_data["transaction_date"] = pd.to_datetime(
            self.silver_data["transaction_date"]
        ).dt.date

        # Create a cross join to compare all possible pairs
        updates = pd.merge(
            self.new_ingestion,
            self.silver_data,
            on=["account_number", "transaction_date", "amount"],
            how="inner",
            suffixes=("_new", "_silver"),
        )

        if updates.empty:
            logger.info(
                "No potential matches found based on account_number, amount, and transaction_date."
            )
            return pd.DataFrame()

        # Filter out transactions with same checksum
        updates = updates[updates["checksum_new"] != updates["checksum_silver"]]

        if updates.empty:
            logger.info("No updates found after filtering by checksum.")
            return pd.DataFrame()

        # Calculate similarities
        updates["levenshtein_distance"] = updates.apply(
            lambda row: levenshtein_distance(
                self._normalize_text(row["concept_new"]),
                self._normalize_text(row["concept_silver"]),
            ),
            axis=1,
        )

        updates["cosine_similarity"] = updates.apply(
            lambda row: self._cosine_similarity(
                row["concept_new"], row["concept_silver"]
            ),
            axis=1,
        )

        updates["jaro_winkler_similarity"] = updates.apply(
            lambda row: self._jaro_winkler_similarity(
                row["concept_new"], row["concept_silver"]
            ),
            axis=1,
        )

        potential_updates = updates[
            (updates["levenshtein_distance"] <= self.levenshtein_threshold)
            | (updates["cosine_similarity"] >= self.cosine_threshold)
            | (updates["jaro_winkler_similarity"] >= self.jaro_winkler_threshold)
        ].copy()

        if potential_updates.empty:
            return pd.DataFrame()

        result_df = potential_updates[
            [
                "company_id_new",
                "account_number",
                "checksum_new",
                "checksum_silver",
                "transaction_date",
                "levenshtein_distance",
                "cosine_similarity",
                "jaro_winkler_similarity",
            ]
        ].rename(
            columns={
                "company_id_new": "company_id",
            }
        )
        return result_df.reset_index(drop=True)


def analyze_transaction_update(
    transaction1,
    transaction2,
    levenshtein_threshold=3,
    cosine_threshold=0.8,
    jaro_winkler_threshold=0.9,
    amount_tolerance=0.01,
):
    """Analyzes two transactions to determine if the second one is an update of
    the first one."""

    # 1. Amount Comparison
    if not compare_amounts(
        transaction1.get("amount"), transaction2.get("amount"), amount_tolerance
    ):
        logger.info("Different amounts. Not considered an update.")
        return False

    # 2. Transaction Date Comparison (should be the same)
    if transaction1.get("transaction_date") != transaction2.get("transaction_date"):
        logger.info("Different transaction dates. Not considered an update.")
        return False

    # 3. Concept Comparison using Levenshtein
    concept1_norm = normalize_text(transaction1.get("concept"))
    concept2_norm = normalize_text(transaction2.get("concept"))
    lev_distance = levenshtein_distance(concept1_norm, concept2_norm)
    logger.info("Levenshtein distance between concepts: %s", lev_distance)
    if lev_distance <= levenshtein_threshold:
        logger.info("Levenshtein distance within threshold.")
        return True

    # 4. Concept Comparison using Cosine Similarity
    concept1 = transaction1.get("concept")
    concept2 = transaction2.get("concept")
    cos_similarity = cosine_similarity(concept1, concept2)
    logger.info("Cosine similarity between concepts: %.4f", cos_similarity)
    if cos_similarity >= cosine_threshold:
        logger.info("Cosine similarity within threshold.")
        return True

    # 5. Concept Comparison using Jaro-Winkler (focused on prefix)
    jaro_winkler_sim = jaro_winkler_similarity(concept1, concept2)
    logger.info("Jaro-Winkler similarity between concepts: %.4f", jaro_winkler_sim)
    if jaro_winkler_sim >= jaro_winkler_threshold:
        logger.info("Jaro-Winkler similarity within threshold.")
        return True

    # 6. Consider the case of significantly different description but identical
    # amount
    if (
        lev_distance > levenshtein_threshold
        and cos_similarity < cosine_threshold
        and jaro_winkler_sim < jaro_winkler_threshold
    ):
        logger.info(
            "Significant change in description with identical amount. "
            "Could be an update."
        )
        return True

    return False