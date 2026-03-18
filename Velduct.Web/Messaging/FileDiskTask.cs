namespace Velduct.Web.Messaging;

// File write task; atomic operation: TempPath → File.Move → FinalPath
public record FileDiskTask(
    string Key,
    string RelativePath,
    long Size,
    DateTime LastWriteTimeUtc,
    string FinalPath,
    string TempPath,
    DateTime OperationStartTime = default
);
