from typing import Optional, Any
from pydantic import BaseModel, Field


class MetadataItem(BaseModel):
    """
    Metadata item model.
    """
    key: str = Field(..., description="Metadata key")
    value: str = Field(..., description="Metadata value")


class CategoryInference(BaseModel):
    """
    Category inference model.
    """
    category_id: int = Field(..., description="Category identifier")
    accuracy: float = Field(..., description="Inference accuracy")


class Enrichments(BaseModel):
    """
    Transaction enrichments.
    """
    category_inference: Optional[CategoryInference] = Field(
        default=None,
        description="Category inference data"
    )


class TransactionMessage(BaseModel):
    """
    Transaction message model from Pub/Sub.
    """
    checksum: str = Field(..., description="Transaction checksum")
    concept: str = Field(..., description="Transaction concept")
    amount: int = Field(..., description="Transaction amount in cents")
    account_number: str = Field(..., description="Account number")
    bank: str = Field(..., description="Bank identifier")
    account_alias: str = Field(..., description="Account alias")
    currency: str = Field(..., description="Transaction currency")
    report_type: str = Field(..., description="Report type")
    extraction_date: str = Field(..., description="Extraction date")
    user_id: str = Field(..., description="User identifier")
    company_id: str = Field(..., description="Company identifier")
    transaction_date: str = Field(..., description="Transaction date")
    reported_remaining: int = Field(
        ..., description="Reported remaining balance"
    )
    metadata: Any = Field(
        default_factory=dict, description="Additional metadata"
    )
    enrichments: Optional[Enrichments] = Field(
        default=None, description="Transaction enrichments"
    ) 