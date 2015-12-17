package main

import (
	"fmt"
	"github.com/niedbalski/go-ceph-blockdevice"
)

func main() {
	connection, err := blockdevice.NewConnection("lxd", "lxd", "", "./ceph.conf")
	if err != nil {
		fmt.Println("Error connecting", err)
	}

	image, err := connection.GetOrCreateImage("foobaritic", 25)
	if err != nil {
		fmt.Println(err)
	}

	if device := image.IsAlreadyMapped(); device != "" {
		fmt.Printf("Image: %s has been already mapped to device:%s", image, device)
	} else {

		device, err := image.MapToDevice("ext4", "/mnt/foo")
		if err != nil {
			fmt.Printf("Error mapping device, Error: %s\n", err)
		} else {
			fmt.Printf("Device: %s - Mounted on: %s\n", device.GetPath(), device.GetMountPoint())
		}
	}
}
