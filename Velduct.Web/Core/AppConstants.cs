namespace Velduct.Web.Core
{
    public static class AppConstants
    {
        public const string UploadExtension = ".upload";
        public const string TempExtension = ".tmp";

        public const string ConfigChunkSizeBytes = "TransferOptions:Network:ChunkSizeBytes";
        public const string ConfigDataDirectory = "TransferOptions:Storage:DataDirectory";
        public const string ConfigTempDirectory = "TransferOptions:Storage:TempDirectory";
        public const string ConfigKestrelBufferOverhead = "TransferOptions:WebSocket:KestrelBufferOverhead";
        public const string ConfigKeepAliveSeconds = "TransferOptions:WebSocket:KeepAliveSeconds";
    }
}
