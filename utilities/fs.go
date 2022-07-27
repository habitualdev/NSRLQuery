package utilities

import "os"

func PrepFilesytem() {
	if _, err := os.Stat("buckets"); err != nil {
		os.Mkdir("buckets", 0700)
	}
}
