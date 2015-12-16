package main

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
	"os/exec"
	"strings"
)

const (
	DefaultPoolName       = ""
	DefaultFileSystemType = "xfs"
)

type Connection struct {
	*rados.Conn
	context  *rados.IOContext
	pool     string
	username string
	cluster  string
}

type Image struct {
	*rbd.Image
	*rbd.ImageInfo
	*Connection
	name string
}

type Device struct {
	path           string
	isMounted      bool
	fileSystemType string
	mountPoint     string
}

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

func (d *Device) GetFileSystemType() (string, error) {
	format, err := RunCommand("blkid", "-o", "value", "-s", "TYPE", d.path)
	if err != nil {
		return "", err
	}
	return format, nil
}

func (d *Device) IsAlreadyFormatted() bool {
	if current, _ := d.GetFileSystemType(); current == d.fileSystemType {
		return true
	}
	return false
}

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

func (d *Device) UnMount() error {
	if _, err := RunCommand("unmount", d.path); err != nil {
		return err
	}
	return nil
}

func NewDevice(image *Image, fsType string) (*Device, error) {
	device, err := RunCommand("rbd", "map", "--id", image.username, "--pool", image.pool, image.name)
	if err != nil {
		return nil, err
	}

	return &Device{device, false, DefaultFileSystemType, ""}, nil
}

func toMegs(size uint64) uint64 {
	return size * 1024 * 1024
}

func RunCommand(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	out, err := cmd.Output()
	return strings.Trim(string(out), " \n"), err
}

func (i *Image) MapToDevice(fsType string) (*Device, error) {
	device, err := NewDevice(i, fsType)
	if err != nil {
		return nil, err
	}
	return device, err
}

func NewImage(image *rbd.Image, connection *Connection, name string) (*Image, error) {
	stat, err := image.Stat()
	if err != nil {
		return nil, err
	}

	err = image.Open(true)
	if err != nil {
		return nil, err
	}

	return &Image{
		image,
		stat,
		connection,
		name,
	}, nil
}

func (c *Connection) GetImageByName(name string) (*Image, error) {
	image := rbd.GetImage(c.context, name)
	if image == nil {
		return nil, fmt.Errorf("Image:%s not found on pool:%s", name, c.pool)
	}

	return NewImage(image, c, name)
}

func (c *Connection) GetOrCreateImage(name string, size uint64) (*Image, error) {
	image, err := c.GetImageByName(name)
	if image != nil {
		return image, nil
	}

	new_image, err := rbd.Create(c.context, name, toMegs(size))
	if err != nil {
		return nil, err
	}

	return NewImage(new_image, c, name)
}

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
		return nil, err
	}

	if configFile != "" {
		err = conn.ReadConfigFile(configFile)
	} else {
		err = conn.ReadDefaultConfigFile()
	}

	if err != nil {
		return nil, err
	}

	context, err := conn.OpenIOContext(pool)
	if err != nil {
		return nil, err
	}

	return &Connection{
		conn,
		context,
		pool,
		username,
		cluster,
	}, nil
}

func (c *Connection) Shutdown() {
	if c.context != nil {
		c.context.Destroy()
	}

	c.Shutdown()
}
