from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from . import runtime


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


class CollectionRunInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    filters: SearchFilters
    requestId: str = Field(min_length=1, max_length=100)


class FinalConfirmationInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    version: int = Field(ge=0, strict=True)
    expectedRevision: int = Field(ge=0, strict=True)
    requestId: str = Field(min_length=1, max_length=100)


class CollectionDraftInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    recordId: str = Field(min_length=1, max_length=100)
    startTime: str = Field(min_length=19, max_length=35)
    durationSeconds: int = Field(ge=60, le=7200, strict=True)
    expectedPlanRevision: int = Field(ge=0, strict=True)
    requestId: str = Field(pattern=r'^[A-Za-z0-9_-]{1,100}$')
    page: int = Field(default=1, ge=1)


class CollectionSubmitInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    recordIds: list[str] = Field(min_length=1, max_length=10000)
    expectedPlanRevision: int = Field(ge=0, strict=True)
    requestId: str = Field(pattern=r'^[A-Za-z0-9_-]{1,100}$')


class PreviewInput(BaseModel):
    searchId: str
    recordIds: list[str] = Field(min_length=1, max_length=10000)


class SubmitInput(BaseModel):
    planId: str
    downloadAfterCollection: bool = False
    confirmed: bool = False


class VideoDownloadLimitInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expectedRevision: int = Field(ge=0, strict=True)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")
    maxConcurrentDownloads: int = Field(ge=1, le=8, strict=True)


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


class RecaptureInput(VersionActionInput):
    mode: Literal["repair", "force"] = "repair"


class CollectionRecaptureInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recordId: str = Field(min_length=1, max_length=256)
    expectedPlanRevision: int = Field(ge=0, strict=True)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")


class DirectoryInput(BaseModel):
    directory: str = Field(min_length=1, max_length=2000)


class SliceClassifyConfirmInput(BaseModel):
    planId: str = Field(pattern=r"^classify:[a-f0-9]{32}$")
    expectedRevision: int = Field(ge=0, strict=True)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")


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
    directory: str = Field(default_factory=lambda: "" if runtime.portable() else "/Volumes/out/视频", min_length=1, max_length=2000)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")


class SlicingSettingsInput(BaseModel):
    expectedRevision: int = Field(ge=0, strict=True)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")
    directories: list[str] = Field(min_length=1, max_length=20)
    magnetDeployDirectory: str = Field(min_length=1, max_length=2000)
    magnetModelPath: str = Field(min_length=1, max_length=2000)
    bedDeployDirectory: str = Field(min_length=1, max_length=2000)
    bedModelPath: str = Field(min_length=1, max_length=2000)
    bedGeometryPath: str = Field(min_length=1, max_length=2000)
    frameToolPath: str = Field(min_length=1, max_length=2000)
    workerPythonPath: str = Field(min_length=1, max_length=2000)
    sharedSampling: bool = True
    maxLocalSlicingProcesses: int = Field(default=3, ge=1, le=8, strict=True)
    sampleIntervalSeconds: int = Field(default=30, ge=1, le=3600)
    magnetIntervalSeconds: int = Field(default=30, ge=1, le=3600)
    bedIntervalSeconds: int = Field(default=30, ge=1, le=3600)
    magnetConfidence: float = Field(default=0.25, ge=0, le=1, allow_inf_nan=False)
    targetImages: int = Field(default=150, ge=100, le=200)
    bedTargetImages: int = Field(default=180, ge=100, le=200)
    targetDatasetIds: dict[str, str | None] = Field(default_factory=lambda: {"27912": None, "27944": None})
    autoArchive: bool = True
    reviewBeforeUpload: bool = False
    sliceOutputDirectory: str = ""
    archiveCompletedDirectory: str = Field(default="切片完成", min_length=1, max_length=100)
    archiveEmptyDirectory: str = Field(default="未检出目标", min_length=1, max_length=100)


class SlicingProcessInput(BaseModel):
    expectedRevision: int = Field(ge=0, strict=True)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")
    maxLocalSlicingProcesses: int = Field(ge=1, le=8, strict=True)


class SliceOutputOrganizationPreviewInput(BaseModel):
    expectedRevision: int = Field(ge=0, strict=True)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")


class SliceOutputOrganizationConfirmInput(BaseModel):
    expectedRevision: int = Field(ge=0, strict=True)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")


class SlicingScanInput(BaseModel):
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")


class SlicingStartInput(BaseModel):
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")
    expectedRevision: int = Field(ge=0, strict=True)
    mode: Literal["pilot", "all", "single"] = "pilot"
    itemId: str | None = Field(default=None, min_length=1, max_length=100)


class SlicingItemActionInput(BaseModel):
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")
    expectedRevision: int = Field(ge=0, strict=True)


class FileCleanupInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")
    confirm: Literal[True]


class WorkspacePreviewInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    parent: str = Field(min_length=1, max_length=4000)
    expectedRevision: int = Field(ge=0)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,100}$")


class WorkspaceActionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    expectedRevision: int = Field(ge=0)
    requestId: str = Field(pattern=r"^[A-Za-z0-9_-]{1,80}$")
    pilot: bool = False


class RecordSelection(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    id: str = Field(min_length=1, max_length=100)
    expectedRevision: int = Field(ge=0)


class SlicingRecordRemoveInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    requestId: str = Field(pattern=r'^[A-Za-z0-9_-]{1,100}$')
    selections: list[RecordSelection] = Field(min_length=1, max_length=5000)


class RecordRestoreInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    requestId: str = Field(pattern=r'^[A-Za-z0-9_-]{1,100}$')
    removalId: str = Field(min_length=1, max_length=100)


class ProcessingContinueInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    requestId: str = Field(pattern=r'^[A-Za-z0-9_-]{1,100}$')
    generation: int = Field(ge=0)
    selections: list[RecordSelection] = Field(default_factory=list, max_length=20)
    packages: list[RecordSelection] = Field(default_factory=list, max_length=20)


class PipelineControlInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    expectedRevision: int = Field(ge=0)
    requestId: str = Field(pattern=r'^[A-Za-z0-9_-]{1,100}$')


class PipelineSelection(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    mediaId: str = Field(min_length=1, max_length=200)
    generation: int = Field(ge=0)
    action: Literal['continue', 'archive', 'pause', 'resume'] = 'continue'
    selections: list[RecordSelection] = Field(default_factory=list, max_length=20)
    packages: list[RecordSelection] = Field(default_factory=list, max_length=20)
    fileId: str | None = Field(default=None, min_length=1, max_length=200)
    itemId: str | None = Field(default=None, min_length=1, max_length=200)
    expectedRevision: int | None = Field(default=None, ge=0)

    @model_validator(mode='after')
    def check_action(self):
        if self.action == 'archive' and (not self.fileId or not self.itemId or self.expectedRevision is None):
            raise ValueError('归档操作缺少文件或视频修订信息。')
        if self.action == 'continue' and not self.selections and not self.packages:
            raise ValueError('没有可继续的记录。')
        return self


class PipelineBatchInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    requestId: str = Field(pattern=r'^[A-Za-z0-9_-]{1,100}$')
    selections: list[PipelineSelection] = Field(min_length=1, max_length=20)


class SlicingBindInput(SlicingItemActionInput):
    path: str = Field(min_length=1, max_length=4000)


class SliceIntervalInput(BaseModel):
    start: float = Field(ge=0, allow_inf_nan=False)
    end: float = Field(ge=0, allow_inf_nan=False)


class SlicingReviewInput(SlicingItemActionInput):
    category: Literal["bed", "magnet"]
    intervals: list[SliceIntervalInput] | None = Field(default=None, max_length=100)
    confirmedNoTarget: bool = False
    intervalSeconds: int | None = Field(default=None, ge=1, le=86400)


class GroupRecordRemoveInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    requestId: str = Field(pattern=r'^[A-Za-z0-9_-]{1,100}$')
    selections: list[RecordSelection] = Field(default_factory=list, max_length=5000)
    receipts: list[RecordSelection] = Field(default_factory=list, max_length=5000)


class RecordDeletionChoice(BaseModel):
    kind: Literal['collection', 'slicing', 'receipts', 'receipt_items', 'collection_runs']
    id: str = Field(min_length=1, max_length=300)
    expectedRevision: int = Field(ge=0)


class RecordDeletionInput(BaseModel):
    requestId: str = Field(min_length=1, max_length=100, pattern=r'^[A-Za-z0-9_-]+$')
    selections: list[RecordDeletionChoice] = Field(min_length=1, max_length=10000)
