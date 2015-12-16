package main

import (
	"fmt"
	"github.com/niedbalski/go-ceph-blockdevice"
)

func main() {
	connection, err := blockdevice.NewConnection("lxd", "lxd", "", "./ceph.conf")
	if err != nil {
		fmt.Println(err)
	}

	image, err := connection.GetOrCreateImage("foobar", 20)
	if err != nil {
		fmt.Println(err)
	}

	device, err := image.MapToDevice("ext4", "/mnt/foo")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Device: %s - Mounted on: %s\n", device.GetPath(), device.GetMountPoint())
}
