from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class AdmissionRule(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    requireRealPlate: bool = True
    minInspectionMinutes: float = Field(default=10, ge=0, le=44640, allow_inf_nan=False)
    requireIntelligentPhotos: bool = True
    minPhotoCount: int = Field(default=1, ge=1, le=10000)
    matchMode: Literal["any", "all"] = "any"


class AdmissionSettingsInput(BaseModel):
    rule: AdmissionRule
    expectedRevision: int = Field(ge=0, strict=True)
    searchId: str | None = Field(default=None, min_length=1, max_length=100)


class SearchFilters(BaseModel):
    dateFrom: str
    dateTo: str
    timeFrom: str = "00:00:00"
    timeTo: str = "23:59:59"
    plates: str = Field(default="", max_length=500)
    station: str = ""
    weightBillNo: str = ""
    timeSource: Literal["checkStartTime"] = "checkStartTime"


class CollectionRule(BaseModel):
    offsetMinutes: int = 5
    durationMinutes: int = 60


class LoginInput(BaseModel):
    account: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1, max_length=500)
    remember: bool = False


class SearchInput(BaseModel):
    filters: SearchFilters


class PreviewInput(BaseModel):
    searchId: str
    recordIds: list[str] = Field(min_length=1, max_length=10000)


class SubmitInput(BaseModel):
    planId: str
    downloadAfterCollection: bool = False


class CorrectionInput(BaseModel):
    startTime: str = Field(min_length=19, max_length=35)
    durationSeconds: int = Field(ge=60, le=7200)
    expectedRevision: int = Field(ge=0)
    requestId: str = Field(min_length=1, max_length=100)


class VersionActionInput(BaseModel):
    version: int = Field(ge=0)
    expectedRevision: int = Field(ge=0)
    requestId: str = Field(min_length=1, max_length=100)
    waitForCompletion: bool = False


class DirectoryInput(BaseModel):
    directory: str = Field(min_length=1, max_length=2000)


class HistoryRemoveInput(BaseModel):
    expectedRevision: int = Field(ge=0)
    requestId: str = Field(min_length=1, max_length=100)


class RecordRemoveInput(HistoryRemoveInput):
    deleteCloud: bool = False
    deleteLocalFiles: bool = False


class VideoSelection(BaseModel):
    id: str = Field(pattern=r"^[a-f0-9]{24}$")
    version: int = Field(ge=0)
    expectedRevision: int = Field(ge=0)


class BulkVideoInput(BaseModel):
    selections: list[VideoSelection] = Field(min_length=1, max_length=500)
    requestId: str = Field(min_length=1, max_length=60)
    deleteCloud: bool = False
    deleteLocalFiles: bool = False


class UploadAuditInput(BaseModel):
    directory: str = Field(min_length=1, max_length=2000)
    recursive: bool = False


class UploadAuditLoginInput(BaseModel):
    account: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1, max_length=500)


class AuditMovePreviewInput(BaseModel):
    reportId: str = Field(min_length=1, max_length=100)
    uploadedDirectory: str = Field(min_length=1, max_length=2000)
    magnetMissingDirectory: str = Field(min_length=1, max_length=2000)
    bedMissingDirectory: str = Field(min_length=1, max_length=2000)
    refreshReport: bool = False


class AuditMoveConfirmInput(BaseModel):
    requestId: str = Field(min_length=1, max_length=100)


class UploadScanInput(BaseModel):
    directory: str = Field(default="/Volumes/out/视频", min_length=1, max_length=2000)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")
