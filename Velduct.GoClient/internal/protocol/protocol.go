package protocol

const (
	CmdPullFile      byte = 0x10
	CmdOfferFile     byte = 0x11
	CmdSkipFile      byte = 0x12
	CmdCheckFiles    byte = 0x13
	CmdBatchPull     byte = 0x14
	CmdDeleteConfirm  byte = 0x15
	CmdRegisterShares byte = 0x16
	CmdFileMtimeAck   byte = 0x17
	CmdFileData      byte = 0x06
	CmdFileDone      byte = 0x07
	SrvPullStream    byte = 0x20

	CmdArchiveStart byte = 0x30
	CmdArchiveData  byte = 0x31
	CmdArchiveDone  byte = 0x32

	CmdHeartbeat byte = 0x40
)

