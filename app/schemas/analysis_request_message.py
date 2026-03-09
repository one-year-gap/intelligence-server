from pydantic import BaseModel, ConfigDict, Field


class AnalysisRequestMessage(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    dispatch_request_id: str = Field(..., alias="dispatchRequestId", min_length=1)
    case_id: int = Field(..., alias="caseId", ge=1)
    analyzer_version: int = Field(..., alias="analyzerVersion", ge=1)
