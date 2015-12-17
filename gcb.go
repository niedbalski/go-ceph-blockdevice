//This package is a general abstraction on top of the official rbd/rados
//libraries in order to make the creation of blockdevices simpler.
package blockdevice

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"os/exec"
	"regexp"
	"strings"
)

const (
	DefaultPoolName       = "rbd"
	DefaultFileSystemType = "xfs"
)

//This struct represents a connection to the ceph cluster
type Connection struct {
	*rados.Conn
	context  *rados.IOContext
	pool     string
	username string
	cluster  string
}

//This struct represents a RBD Image
type Image struct {
	*rbd.Image
	*rbd.ImageInfo
	*Connection
	name string
}

//This structure represents a local device mapped on the system.
type Device struct {
	path           string
	isMounted      bool
	fileSystemType string
	mountPoint     string
}

//Getter method for path
func (d *Device) GetPath() string {
	return d.path
}

/*
Getter method for mountpoint
*/
func (d *Device) GetMountPoint() string {
	return d.mountPoint
}

/*
This method mounts a `Device` on the given Mountpoint, it returns
and error if is already mounted or has been already formatted.
*/
func (d *Device) Mount(mountPoint string) (string, error) {
	if d.isMounted && d.mountPoint == mountPoint {
		return "", fmt.Errorf("Device: %s is already mounted on path: %s", d.path, d.mountPoint)
	}

	if !d.IsAlreadyFormatted() {
		if err := d.Format(); err != nil {
			return "", err
		}
	}

	if _, err := RunCommand("mount", "-t", d.fileSystemType, d.path, mountPoint); err != nil {
		return "", err
	}

	d.isMounted = true
	return mountPoint, nil
}

/*
This method formats a given device with the specific filesystem type
*/
func (d *Device) Format() error {
	mkfs, err := exec.LookPath("mkfs." + d.fileSystemType)
	if err != nil {
		return fmt.Errorf("Cannot format device:%s, Error: %s", d.path, err)
	}

	if _, err = RunCommand(mkfs, d.path); err != nil {
		return fmt.Errorf("Cannot format device:%s, Error: %s", d.path, err)
	}
	return nil
}

/*
This method returns the filesystem type using blkid of a given device.
*/
func (d *Device) GetFileSystemType() (string, error) {
	format, err := RunCommand("blkid", "-o", "value", "-s", "TYPE", d.path)
	if err != nil {
		return "", err
	}
	return format, nil
}

/*
This method checks if the current filesystem for a given device
matches the expected fileSystemType.
*/
func (d *Device) IsAlreadyFormatted() bool {
	if current, _ := d.GetFileSystemType(); current == d.fileSystemType {
		return true
	}
	return false
}

/*
This method unmaps a device using the 'rbd unmap' command
*/
func (d *Device) UnMap() error {
	if d.isMounted {
		if err := d.UnMount(); err != nil {
			return err
		}
	}

	if _, err := RunCommand("rbd", "unmap", d.path); err != nil {
		return err
	}
	return nil
}

/*
This method unmounts the device from the current mounting path.
*/
func (d *Device) UnMount() error {
	if _, err := RunCommand("unmount", d.path); err != nil {
		return err
	}
	return nil
}

/*
This method is a contructor for `Device` Objects.
*/
func NewDevice(image *Image, fsType string, mountPoint string) (*Device, error) {
	device, err := RunCommand("rbd", "map", "--id", image.username, "--pool", image.pool, image.name)
	if err != nil {
		return nil, err
	}

	if fsType == "" {
		fsType = DefaultFileSystemType
	}

	new_device := &Device{device, false, fsType, mountPoint}

	if err = new_device.Format(); err != nil {
		return nil, err
	}

	if mountPoint != "" {
		if _, err = new_device.Mount(mountPoint); err != nil {
			return nil, err
		}

		new_device.isMounted = true
	}

	return new_device, nil
}

/*
This is a helper method that transform units
from bytes to megas.
*/
func toMegs(size uint64) uint64 {
	return size * 1024 * 1024
}

/*
This is a helper method for running a command and returning
the output.
*/
func RunCommand(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	out, err := cmd.Output()
	return strings.Trim(string(out), " \n"), err
}

/*
This method creates a new rados device (if available on the system), formats
it on the given `fsType` and mount it on the given `mountPoint`
*/
func (i *Image) MapToDevice(fsType string, mountPoint string) (*Device, error) {
	device, err := NewDevice(i, fsType, mountPoint)
	if err != nil {
		return nil, fmt.Errorf("Cannot create new device for image: %s, Error: %s", i.name, err)
	}
	return device, err
}

/*
This methods returns the current device path of a given
device is if mapped, otherwise it returns an empty string
*/
func (i *Image) IsAlreadyMapped() string {
	devices, err := i.GetMappedDevices()
	if err != nil {
		return ""
	}

	if path, ok := devices[i.name]; ok {
		return path
	}

	return ""
}

/*
This is a constructor for `Image`, this also opens an image descriptor,
and performs an Stat on it.
*/
func NewImage(image *rbd.Image, connection *Connection, name string) (*Image, error) {
	if err := image.Open(true); err != nil {
		return nil, err
	}

	stat, err := image.Stat()
	if err != nil {
		return nil, fmt.Errorf("Cannot state image: %s, Error: %s", name, err)
	}

	return &Image{
		image,
		stat,
		connection,
		name,
	}, nil
}

/*
This method retrieves an image from the pool given
the `name`
*/
func (c *Connection) GetImageByName(name string) (*Image, error) {
	image := rbd.GetImage(c.context, name)
	if image == nil {
		return nil, fmt.Errorf("Image:%s not found on pool:%s", name, c.pool)
	}

	return NewImage(image, c, name)
}

/*
This method tries to fetch the given `name` from the ceph pool,
if is not found it creates a new one using the given `size` parameter.
*/
func (c *Connection) GetOrCreateImage(name string, size uint64) (*Image, error) {
	if image, _ := c.GetImageByName(name); image != nil {
		return image, nil
	}

	new_image, err := rbd.Create(c.context, name, toMegs(size))
	if err != nil {
		return nil, fmt.Errorf("Cannot create image:%s of size:%d on pool: %s, Error: %s", name, toMegs(size), c.pool, err)
	}

	return NewImage(new_image, c, name)
}

/*
Creates a new connection to a Ceph cluster, this connection
could be shutdown by defering the `Shutdown` method.
*/
func NewConnection(username string, pool string, cluster string, configFile string) (*Connection, error) {
	var conn *rados.Conn
	var err error

	if cluster != "" && username != "" {
		conn, err = rados.NewConnWithClusterAndUser(cluster, username)
	} else if username != "" {
		conn, err = rados.NewConnWithUser(username)
	} else {
		conn, err = rados.NewConn()
	}

	if err != nil {
		return nil, fmt.Errorf("Error creating a connection with ceph, Error: %s", err)
	}

	if configFile != "" {
		err = conn.ReadConfigFile(configFile)
	} else {
		err = conn.ReadDefaultConfigFile()
	}

	if err != nil {
		return nil, fmt.Errorf("Error reading ceph configuration, Error: %s", err)
	}

	err = conn.Connect()
	if err != nil {
		return nil, fmt.Errorf("Error connecting to ceph, Error: %s", err)
	}

	if pool == "" {
		pool = DefaultPoolName
	}

	context, err := conn.OpenIOContext(pool)
	if err != nil {
		return nil, fmt.Errorf("Error opening a IO Context with ceph, Error; %s", err)
	}

	return &Connection{
		conn,
		context,
		pool,
		username,
		cluster,
	}, nil
}

/*
This method lists all the mapped devices available on the system
as seen by the output of the 'rbd showmapped' command
*/
func (c *Connection) GetMappedDevices() (map[string]string, error) {
	output, err := RunCommand("rbd", "showmapped")
	if err != nil {
		return nil, err
	}

	devices := make(map[string]string)
	for _, line := range strings.Split(output, "\n") {
		if matches, _ := regexp.MatchString("[0-9]+.*['\\/dev\\/rbd']+", line); matches == true {
			line := strings.Split(line, " ")
			devices[line[4]] = line[9]
		}
	}

	return devices, nil
}

/*
This method destroys the connection context and the
connection itself.
*/
func (c *Connection) Shutdown() {
	if c.context != nil {
		c.context.Destroy()
	}

	c.Shutdown()
}
