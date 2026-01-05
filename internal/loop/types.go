package loop

// Loop device flags from <linux/loop.h>
const (
	LoFlagsReadOnly  = 1 << 0
	LoFlagsAutoclear = 1 << 2
	LoFlagsPartscan  = 1 << 3
	LoFlagsDirectIO  = 1 << 4
)

// LoopInfo64 is the loop device info structure for LOOP_SET_STATUS64/LOOP_GET_STATUS64.
// This matches the kernel's struct loop_info64 from <linux/loop.h>.
type LoopInfo64 struct {
	Device         uint64
	Inode          uint64
	Rdevice        uint64
	Offset         uint64
	SizeLimit      uint64
	Number         uint32
	EncryptType    uint32
	EncryptKeySize uint32
	Flags          uint32
	FileName       [64]byte
	CryptName      [64]byte
	EncryptKey     [32]byte
	Init           [2]uint64
}

// Config holds configuration options for setting up a loop device.
type Config struct {
	// ReadOnly sets the loop device as read-only.
	ReadOnly bool
	// Autoclear automatically detaches the loop device when the last user closes it.
	Autoclear bool
	// DirectIO enables direct I/O mode.
	DirectIO bool
	// Serial is an optional serial number for the loop device (Linux 5.17+).
	// This is written to /sys/block/loopN/loop/serial for udev identification.
	Serial string
	// Offset specifies the offset in the backing file where data starts.
	Offset uint64
	// SizeLimit limits the size of the loop device (0 = entire file).
	SizeLimit uint64
}

// Device represents an attached loop device.
type Device struct {
	// Path is the device path (e.g., "/dev/loop0").
	Path string
	// Number is the loop device number.
	Number int
}

// BackingFile returns the backing file path from the loop device info.
func (info *LoopInfo64) BackingFile() string {
	// Find null terminator
	for i, b := range info.FileName {
		if b == 0 {
			return string(info.FileName[:i])
		}
	}
	return string(info.FileName[:])
}
